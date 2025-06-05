using System;
using System.Linq;
using cAlgo.API;
using cAlgo.API.Indicators;
using cAlgo.API.Internals;
using cAlgo.Indicators;

namespace cAlgo.Robots
{
    [Robot(TimeZone = TimeZones.UTC, AccessRights = AccessRights.None)]
    public class SilverBulletBot : Robot
    {
        [Parameter("Minutes Before Session for Liquidity", DefaultValue = 10)]
        public int MinutesBeforeSessionForLiquidity { get; set; }

        [Parameter("Risk Percentage", DefaultValue = 1.0, MinValue = 0.1, MaxValue = 5.0)]
        public double RiskPercentage { get; set; }

        [Parameter("Risk Reward Ratio", DefaultValue = 1.5, MinValue = 0.5)]
        public double RiskRewardRatio { get; set; }

        [Parameter("Context TimeFrame", DefaultValue = "m15")]
        public string ContextTimeFrameString { get; set; }

        [Parameter("Swing Lookback Period", DefaultValue = 20, MinValue = 3)]
        public int SwingLookbackPeriod { get; set; }

        [Parameter("Swing Candles", DefaultValue = 1, MinValue = 1, MaxValue = 3)] // Number of candles on each side for swing definition
        public int SwingCandles { get; set; }
        
        // New York TimeZoneInfo
        private TimeZoneInfo _newYorkTimeZone;
        private TimeFrame _contextTimeFrame;

        // Session timing and liquidity state
        private DateTime _session1ActualStartNY, _session2ActualStartNY, _session3ActualStartNY;
        private DateTime _session1ActualEndNY, _session2ActualEndNY, _session3ActualEndNY;
        private bool _liquidityIdentifiedForSession1, _liquidityIdentifiedForSession2, _liquidityIdentifiedForSession3;
        
        private double _currentLiquidityHigh;
        private double _currentLiquidityLow;
        private DateTime _currentLiquiditySourceBarTimeNY;
        private DateTime _lastDateProcessedForSessionTimes = DateTime.MinValue;

        private enum SweepType { None, HighSwept, LowSwept }
        private SweepType _lastSweepType = SweepType.None;
        private double _lastSweptLiquidityLevel;
        private DateTime _timeOfLastSweepNY; // Precise time of sweep (tick time)
        
        // Sweep Bar details on Context TimeFrame
        private DateTime _sweepBarOpenTimeNY; // Open time of the M15 bar where sweep occurred (NY)
        private double _sweepBarActualHighOnContextTF;
        private double _sweepBarActualLowOnContextTF;


        private double _relevantSwingLevelForBOS;
        private DateTime _relevantSwingBarTimeNY; // Open time of the identified swing bar (NY)

        private double _bosLevel;
        private DateTime _bosTimeNY;

        private string _currentPendingOrderLabel; // To store the label of the active pending order
        private double _lastFvgDetermined_Low;    // Stores Low of the identified FVG range for entry
        private double _lastFvgDetermined_High;   // Stores High of the identified FVG range for entry
        private double _fvgBarN_Low;              // Stores Low of the N bar (first bar of FVG pattern) of the found FVG
        private double _fvgBarN_High;             // Stores High of the N bar (first bar of FVG pattern) of the found FVG

        private struct SessionInfo
        {
            public bool IsActivePeriod; // True if current time is within [SessionStart-Buffer, SessionEnd)
            public DateTime ActualStartNY; // Actual start time of the session (e.g., 03:00:00)
            public DateTime ActualEndNY;   // Actual end time of the session (e.g., 04:00:00)
            public int SessionNumber;    // 1, 2, or 3. 0 if not in any session\'s active period.
        }

        protected override void OnStart()
        {
            // Initialize New York TimeZoneInfo
            try
            {
                _newYorkTimeZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
                if (_newYorkTimeZone == null) 
                {
                    _newYorkTimeZone = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
                }
            }
            catch (TimeZoneNotFoundException)
            {
                Print("New York Time Zone not found. Please check TimeZone ID. Bot will use UTC.");
                _newYorkTimeZone = TimeZoneInfo.Utc; 
            }
            catch (Exception ex)
            {
                Print($"Error initializing New York Time Zone: {ex.Message}. Bot will use UTC.");
                _newYorkTimeZone = TimeZoneInfo.Utc;
            }

            try
            {
                _contextTimeFrame = TimeFrame.Parse(ContextTimeFrameString);
            }
            catch (ArgumentException)
            {
                Print($"Error parsing ContextTimeFrameString: \'{ContextTimeFrameString}\'. Defaulting to m15.");
                _contextTimeFrame = TimeFrame.Minute15;
            }

            Print("Silver Bullet Bot Started.");
            Print("Trading Sessions (NY Time): 03:00-04:00, 10:00-11:00, 14:00-15:00");
            Print($"Liquidity lookback: {MinutesBeforeSessionForLiquidity} minutes before session start.");
            Print($"Risk per trade: {RiskPercentage}%, RR: {RiskRewardRatio}");
            Print($"Context TimeFrame: {_contextTimeFrame}");
            Print($"Swing Lookback: {SwingLookbackPeriod} bars, Swing Candles: {SwingCandles}");

            PendingOrders.Filled += OnPendingOrderFilled;
            PendingOrders.Cancelled += OnPendingOrderCancelled;
            Positions.Opened += OnPositionOpened;
            Positions.Closed += OnPositionClosed;
        }

        protected override void OnTick()
        {
            var currentTimeNY = GetNewYorkTime(Server.Time);
            DailyResetAndSessionTimeUpdate(currentTimeNY);

            SessionInfo currentSession = GetCurrentSessionInfo(currentTimeNY);

            if (_lastSweepType != SweepType.None)
            {
                var contextBars = MarketData.GetBars(_contextTimeFrame);
                if (contextBars.Count == 0) return;

                if (_relevantSwingLevelForBOS == 0) 
                {
                    IdentifyRelevantSwingForBOS(contextBars);
                }
                
                if (_relevantSwingLevelForBOS != 0) 
                {
                    CheckForBOS(currentTimeNY, contextBars);
                }
            }
            else if (currentSession.IsActivePeriod)
            {
                bool relevantLiquidityFlag = GetLiquidityIdentifiedFlag(currentSession.SessionNumber);

                if (!relevantLiquidityFlag && currentTimeNY >= currentSession.ActualStartNY)
                {
                    IdentifyLiquidityLevels(currentSession.ActualStartNY, currentSession.SessionNumber);
                }

                relevantLiquidityFlag = GetLiquidityIdentifiedFlag(currentSession.SessionNumber);

                if (relevantLiquidityFlag && (_currentLiquidityHigh != 0 || _currentLiquidityLow != 0))
                {
                    CheckForLiquiditySweep(currentTimeNY);
                }
            }
            else
            {
                if (_currentLiquidityHigh != 0 || _currentLiquidityLow != 0)
                {
                    if (!_liquidityIdentifiedForSession1 && !_liquidityIdentifiedForSession2 && !_liquidityIdentifiedForSession3 && _lastSweepType == SweepType.None)
                    {
                        _currentLiquidityHigh = 0;
                        _currentLiquidityLow = 0;
                    }
                }
                if (_lastSweepType != SweepType.None && _relevantSwingLevelForBOS == 0) // If sweep happened but no swing found, reset to re-evaluate liquidity
                {
                     // This condition is handled inside IdentifyRelevantSwingForBOS now
                }
            }
        }

        protected override void OnStop()
        {
            Print("Silver Bullet Bot Stopped.");
            if (!string.IsNullOrEmpty(_currentPendingOrderLabel))
            {
                var pendingOrder = PendingOrders.FirstOrDefault(o => o.Label == _currentPendingOrderLabel);
                if (pendingOrder != null)
                {
                    CancelPendingOrderAsync(pendingOrder, (result) => 
                    {
                        if(result.IsSuccessful) Print($"Pending order {pendingOrder.Label} cancelled on bot stop.");
                        else Print($"Failed to cancel pending order {pendingOrder.Label} on bot stop: {result.Error}");
                    });
                }
            }
            PendingOrders.Filled -= OnPendingOrderFilled;
            PendingOrders.Cancelled -= OnPendingOrderCancelled;
            Positions.Opened -= OnPositionOpened;
            Positions.Closed -= OnPositionClosed;
        }

        private DateTime GetNewYorkTime(DateTime serverTime)
        {
            if (serverTime.Kind == DateTimeKind.Unspecified)
            {
                serverTime = DateTime.SpecifyKind(serverTime, DateTimeKind.Utc);
            }
            else if (serverTime.Kind == DateTimeKind.Local)
            {
                serverTime = serverTime.ToUniversalTime(); 
            }
            return TimeZoneInfo.ConvertTimeFromUtc(serverTime, _newYorkTimeZone);
        }

        private void DailyResetAndSessionTimeUpdate(DateTime currentTimeNY)
        {
            if (_lastDateProcessedForSessionTimes.Date != currentTimeNY.Date)
            {
                Print($"New trading day: {currentTimeNY.Date.ToShortDateString()}. Updating session times and resetting liquidity flags.");
                _lastDateProcessedForSessionTimes = currentTimeNY.Date;

                _session1ActualStartNY = currentTimeNY.Date + TimeSpan.Parse("03:00");
                _session1ActualEndNY = currentTimeNY.Date + TimeSpan.Parse("04:00");
                _session2ActualStartNY = currentTimeNY.Date + TimeSpan.Parse("10:00");
                _session2ActualEndNY = currentTimeNY.Date + TimeSpan.Parse("11:00");
                _session3ActualStartNY = currentTimeNY.Date + TimeSpan.Parse("14:00");
                _session3ActualEndNY = currentTimeNY.Date + TimeSpan.Parse("15:00");

                _liquidityIdentifiedForSession1 = false;
                _liquidityIdentifiedForSession2 = false;
                _liquidityIdentifiedForSession3 = false;
                _currentLiquidityHigh = 0;
                _currentLiquidityLow = 0;
                _lastSweepType = SweepType.None; // Reset sweep type for new day
                _relevantSwingLevelForBOS = 0;
                 _sweepBarOpenTimeNY = DateTime.MinValue;
                _sweepBarActualHighOnContextTF = 0;
                _sweepBarActualLowOnContextTF = 0;
            }
        }

        private SessionInfo GetCurrentSessionInfo(DateTime currentTimeNY)
        {
            TimeSpan currentTimeOfDay = currentTimeNY.TimeOfDay;
            TimeSpan preBuffer = TimeSpan.FromMinutes(MinutesBeforeSessionForLiquidity);

            if (currentTimeNY >= _session1ActualStartNY.Subtract(preBuffer) && currentTimeNY < _session1ActualEndNY)
            {
                return new SessionInfo { IsActivePeriod = true, ActualStartNY = _session1ActualStartNY, ActualEndNY = _session1ActualEndNY, SessionNumber = 1 };
            }
            if (currentTimeNY >= _session2ActualStartNY.Subtract(preBuffer) && currentTimeNY < _session2ActualEndNY)
            {
                return new SessionInfo { IsActivePeriod = true, ActualStartNY = _session2ActualStartNY, ActualEndNY = _session2ActualEndNY, SessionNumber = 2 };
            }
            if (currentTimeNY >= _session3ActualStartNY.Subtract(preBuffer) && currentTimeNY < _session3ActualEndNY)
            {
                return new SessionInfo { IsActivePeriod = true, ActualStartNY = _session3ActualStartNY, ActualEndNY = _session3ActualEndNY, SessionNumber = 3 };
            }
            return new SessionInfo { IsActivePeriod = false, SessionNumber = 0 };
        }
        
        private bool GetLiquidityIdentifiedFlag(int sessionNumber)
        {
            switch (sessionNumber)
            {
                case 1: return _liquidityIdentifiedForSession1;
                case 2: return _liquidityIdentifiedForSession2;
                case 3: return _liquidityIdentifiedForSession3;
                default: return false;
            }
        }

        private void SetLiquidityIdentifiedFlag(int sessionNumber, bool value)
        {
            switch (sessionNumber)
            {
                case 1: _liquidityIdentifiedForSession1 = value; break;
                case 2: _liquidityIdentifiedForSession2 = value; break;
                case 3: _liquidityIdentifiedForSession3 = value; break;
            }
        }

        private void IdentifyLiquidityLevels(DateTime sessionActualStartNY, int sessionNumber)
        {
            var contextBars = MarketData.GetBars(_contextTimeFrame);
            if (contextBars.Count == 0)
            {
                Print("Error: Context bars series is empty. Cannot identify liquidity.");
                return;
            }

            DateTime timeForLiquidityBarTargetNY = sessionActualStartNY.AddTicks(-1);
            DateTime timeForLiquidityBarTargetUtc;

            try
            {
                DateTime nyDateTimeToConvert = timeForLiquidityBarTargetNY;
                timeForLiquidityBarTargetUtc = TimeZoneInfo.ConvertTime(nyDateTimeToConvert, _newYorkTimeZone, TimeZoneInfo.Utc);
            }
            catch (Exception ex)
            {
                Print($"Error converting liquidity target time to UTC for session {sessionNumber}: {ex.Message}. Original NY time: {timeForLiquidityBarTargetNY}, Kind: {timeForLiquidityBarTargetNY.Kind}");
                return;
            }

            int liquidityBarIndex = contextBars.OpenTimes.GetIndexByTime(timeForLiquidityBarTargetUtc);

            if (liquidityBarIndex >= 0 && liquidityBarIndex < contextBars.Count)
            {
                _currentLiquidityHigh = contextBars.HighPrices[liquidityBarIndex];
                _currentLiquidityLow = contextBars.LowPrices[liquidityBarIndex];
                _currentLiquiditySourceBarTimeNY = GetNewYorkTime(contextBars.OpenTimes[liquidityBarIndex]);
                
                SetLiquidityIdentifiedFlag(sessionNumber, true);
                Print($"Session {sessionNumber} ({sessionActualStartNY:HH:mm} NY): Liquidity identified from bar {_currentLiquiditySourceBarTimeNY:yyyy-MM-dd HH:mm} NY. High: {Math.Round(_currentLiquidityHigh, Symbol.Digits)}, Low: {Math.Round(_currentLiquidityLow, Symbol.Digits)}");
            }
            else
            {
                Print($"Error for Session {sessionNumber} ({sessionActualStartNY:HH:mm} NY): Could not find suitable bar index ({liquidityBarIndex}) from {contextBars.Count} bars to identify liquidity. Target Time (NY): {timeForLiquidityBarTargetNY:yyyy-MM-dd HH:mm:ss.fff}, Target Time (UTC): {timeForLiquidityBarTargetUtc:yyyy-MM-dd HH:mm:ss.fff}");
            }
        }

        private void CheckForLiquiditySweep(DateTime currentTimeNY)
        {
            if ((_currentLiquidityHigh == 0 && _currentLiquidityLow == 0) || _lastSweepType != SweepType.None) return;

            double currentAsk = Symbol.Ask;
            double currentBid = Symbol.Bid;
            SweepType detectedSweepThisTick = SweepType.None;

            if (_currentLiquidityHigh != 0 && currentAsk > _currentLiquidityHigh)
            {
                Print($"HIGH LIQUIDITY SWEPT at {_currentLiquidityHigh}. Price: {currentAsk}, Time: {currentTimeNY:HH:mm:ss} NY");
                detectedSweepThisTick = SweepType.HighSwept;
                _lastSweptLiquidityLevel = _currentLiquidityHigh;
            }
            else if (_currentLiquidityLow != 0 && currentBid < _currentLiquidityLow)
            {
                Print($"LOW LIQUIDITY SWEPT at {_currentLiquidityLow}. Price: {currentBid}, Time: {currentTimeNY:HH:mm:ss} NY");
                detectedSweepThisTick = SweepType.LowSwept;
                _lastSweptLiquidityLevel = _currentLiquidityLow;
            }

            if (detectedSweepThisTick != SweepType.None)
            {
                _lastSweepType = detectedSweepThisTick;
                _timeOfLastSweepNY = currentTimeNY; // Tick time of sweep
                
                _currentLiquidityHigh = 0; 
                _currentLiquidityLow = 0;

                var contextBars = MarketData.GetBars(_contextTimeFrame);
                if (contextBars.Count > 0)
                {
                    DateTime sweepTickTimeUtc = TimeZoneInfo.ConvertTime(_timeOfLastSweepNY, _newYorkTimeZone, TimeZoneInfo.Utc);
                    int sweepContextBarIndex = contextBars.OpenTimes.GetIndexByTime(sweepTickTimeUtc);

                    if (sweepContextBarIndex >= 0 && sweepContextBarIndex < contextBars.Count)
                    {
                        _sweepBarOpenTimeNY = GetNewYorkTime(contextBars.OpenTimes[sweepContextBarIndex]);
                        _sweepBarActualHighOnContextTF = contextBars.HighPrices[sweepContextBarIndex];
                        _sweepBarActualLowOnContextTF = contextBars.LowPrices[sweepContextBarIndex];
                        Print($"Sweep occurred on M15 bar: {_sweepBarOpenTimeNY:yyyy-MM-dd HH:mm} NY, H: {_sweepBarActualHighOnContextTF}, L: {_sweepBarActualLowOnContextTF}");
                        IdentifyRelevantSwingForBOS(contextBars); 
                    }
                    else
                    {
                        Print($"Error: Could not find context bar for sweep time {_timeOfLastSweepNY:HH:mm:ss} (UTC: {sweepTickTimeUtc}). Index: {sweepContextBarIndex}. Resetting sweep.");
                        _lastSweepType = SweepType.None; // Reset as we\' can\'t define the sweep bar
                    }
                }
                else
                {
                    Print("Cannot identify swing for BOS or sweep bar details: Context bars are empty.");
                    _lastSweepType = SweepType.None; // Reset if we\' can\'t get bars
                }
            }
        }

        private bool IsSwingHigh(int barIndex, Bars series, int swingCandles = 1)
        {
            if (barIndex < swingCandles || barIndex >= series.Count - swingCandles)
                return false; 

            double centralHigh = series.HighPrices[barIndex];
            for (int i = 1; i <= swingCandles; i++)
            {
                if (series.HighPrices[barIndex - i] >= centralHigh || series.HighPrices[barIndex + i] >= centralHigh)
                    return false;
            }
            return true;
        }

        private bool IsSwingLow(int barIndex, Bars series, int swingCandles = 1)
        {
            if (barIndex < swingCandles || barIndex >= series.Count - swingCandles)
                return false; 

            double centralLow = series.LowPrices[barIndex];
            for (int i = 1; i <= swingCandles; i++)
            {
                if (series.LowPrices[barIndex - i] <= centralLow || series.LowPrices[barIndex + i] <= centralLow)
                    return false;
            }
            return true;
        }

        private void IdentifyRelevantSwingForBOS(Bars contextBars)
        {
            if (_lastSweepType == SweepType.None || contextBars.Count == 0 || _sweepBarOpenTimeNY == DateTime.MinValue)
            {
                Print("IdentifyRelevantSwingForBOS: Pre-conditions not met (No sweep, no bars, or sweep bar time not set).");
                // If sweep bar time is not set, it implies an issue in CheckForLiquiditySweep, reset sweep.
                if (_sweepBarOpenTimeNY == DateTime.MinValue && _lastSweepType != SweepType.None) _lastSweepType = SweepType.None;
                return;
            }

            DateTime sweepBarOpenTimeUtc = TimeZoneInfo.ConvertTime(_sweepBarOpenTimeNY, _newYorkTimeZone, TimeZoneInfo.Utc);
            int sweepBarIndex = contextBars.OpenTimes.GetIndexByTime(sweepBarOpenTimeUtc); 
            
            if (sweepBarIndex < 0) 
            {
                Print($"Error: Could not find sweep bar index for time {_sweepBarOpenTimeNY} (UTC: {sweepBarOpenTimeUtc}). Resetting sweep.");
                _lastSweepType = SweepType.None;
                return;
            }
            
            // _sweepBarTimeNY is already set with the M15 bar\'s open time from CheckForLiquiditySweep.

            _relevantSwingLevelForBOS = 0; 
            _relevantSwingBarTimeNY = DateTime.MinValue;

            int searchStartIndex = sweepBarIndex - 1;
            int searchEndIndex = Math.Max(0, sweepBarIndex - SwingLookbackPeriod);

            Print($"Identifying relevant swing: Sweep Bar NY Time {_sweepBarOpenTimeNY:HH:mm}, Index {sweepBarIndex}. Searching from index {searchStartIndex} to {searchEndIndex}.");

            if (_lastSweepType == SweepType.HighSwept) 
            {
                for (int i = searchStartIndex; i >= searchEndIndex; i--)
                {
                    if (IsSwingLow(i, contextBars, SwingCandles))
                    {
                        _relevantSwingLevelForBOS = contextBars.LowPrices[i];
                        _relevantSwingBarTimeNY = GetNewYorkTime(contextBars.OpenTimes[i]);
                        Print($"Relevant Swing Low for BOS identified at {_relevantSwingLevelForBOS} (Bar: {_relevantSwingBarTimeNY:yyyy-MM-dd HH:mm} NY) after High Sweep.");
                        return;
                    }
                }
                Print("No relevant Swing Low found within lookback period for BOS after High Sweep. Resetting sweep state.");
            }
            else if (_lastSweepType == SweepType.LowSwept) 
            {
                for (int i = searchStartIndex; i >= searchEndIndex; i--)
                {
                    if (IsSwingHigh(i, contextBars, SwingCandles))
                    {
                        _relevantSwingLevelForBOS = contextBars.HighPrices[i];
                        _relevantSwingBarTimeNY = GetNewYorkTime(contextBars.OpenTimes[i]);
                        Print($"Relevant Swing High for BOS identified at {_relevantSwingLevelForBOS} (Bar: {_relevantSwingBarTimeNY:yyyy-MM-dd HH:mm} NY) after Low Sweep.");
                        return;
                    }
                }
                Print("No relevant Swing High found within lookback period for BOS after Low Sweep. Resetting sweep state.");
            }
            
            if(_relevantSwingLevelForBOS == 0)
            {
                _lastSweepType = SweepType.None; // Reset sweep if no relevant swing is found
                _sweepBarOpenTimeNY = DateTime.MinValue;
                _sweepBarActualHighOnContextTF = 0;
                _sweepBarActualLowOnContextTF = 0;
            }
        }

        private void CheckForBOS(DateTime currentTimeNY, Bars contextBars)
        {
            if (_lastSweepType == SweepType.None || _relevantSwingLevelForBOS == 0 || contextBars.Count == 0 || _sweepBarOpenTimeNY == DateTime.MinValue)
            {
                return;
            }

            DateTime sweepBarTimeUtc = TimeZoneInfo.ConvertTime(_sweepBarOpenTimeNY, _newYorkTimeZone, TimeZoneInfo.Utc);
            int firstBarToCheckForBOSIndex = contextBars.OpenTimes.GetIndexByTime(sweepBarTimeUtc);
            if (firstBarToCheckForBOSIndex < 0) { // Should not happen if _sweepBarOpenTimeNY is valid
                 Print($"Error finding sweep bar index in CheckForBOS for time: {_sweepBarOpenTimeNY}. Resetting.");
                 _lastSweepType = SweepType.None;
                 return;
            }
            // We start checking for BOS from the bar *after* the sweep bar, or the sweep bar itself if it\'s a very strong move.
            // The FVG can form on the sweep bar itself or subsequent bars.
            // The loop for BOS bar candidates will iterate from current closed bars backwards.
            // The loop for FVG will then iterate from the BOS bar backwards to the sweep bar.

            for (int bosCandidateBarIndex = contextBars.Count - 2; bosCandidateBarIndex > firstBarToCheckForBOSIndex; bosCandidateBarIndex--) 
            {
                //Ensure bosCandidateBarIndex is not before or at the swing bar itself for BOS check.
                 DateTime bosCandidateBarOpenTimeNY = GetNewYorkTime(contextBars.OpenTimes[bosCandidateBarIndex]);
                if (bosCandidateBarOpenTimeNY <= _relevantSwingBarTimeNY) 
                    continue;

                bool bosConfirmedThisBar = false;
                if (_lastSweepType == SweepType.HighSwept && contextBars.ClosePrices[bosCandidateBarIndex] < _relevantSwingLevelForBOS) 
                {
                    _bosLevel = _relevantSwingLevelForBOS; 
                    _bosTimeNY = bosCandidateBarOpenTimeNY;
                    Print($"BEARISH BOS DETECTED at bar {_bosTimeNY:yyyy-MM-dd HH:mm} NY. Structure broken below Swing Low: {Math.Round(_relevantSwingLevelForBOS, Symbol.Digits)}. Sweep was: {_lastSweepType}");
                    bosConfirmedThisBar = true;
                }
                else if (_lastSweepType == SweepType.LowSwept && contextBars.ClosePrices[bosCandidateBarIndex] > _relevantSwingLevelForBOS) 
                {
                    _bosLevel = _relevantSwingLevelForBOS;
                    _bosTimeNY = bosCandidateBarOpenTimeNY;
                    Print($"BULLISH BOS DETECTED at bar {_bosTimeNY:yyyy-MM-dd HH:mm} NY. Structure broken above Swing High: {Math.Round(_relevantSwingLevelForBOS, Symbol.Digits)}. Sweep was: {_lastSweepType}");
                    bosConfirmedThisBar = true;
                }

                if (bosConfirmedThisBar)
                {
                    // Now search for FVG from bosCandidateBarIndex down to firstBarToCheckForBOSIndex (sweep bar index)
                    for (int fvgSearchIndex = bosCandidateBarIndex; fvgSearchIndex >= firstBarToCheckForBOSIndex; fvgSearchIndex--)
                    {
                        // Ensure we have enough bars for FVG (N, N+1, N+2 pattern means fvgSearchIndex must be at least index 1 for N-1)
                        if (fvgSearchIndex < 1 || fvgSearchIndex + 1 >= contextBars.Count) continue;

                        double fvgLowBoundary, fvgHighBoundary;
                        bool fvgFound = false;

                        if (_lastSweepType == SweepType.HighSwept) // Bearish BOS, look for Bearish FVG
                        {
                            if (FindBearishFVG(fvgSearchIndex, contextBars, out fvgLowBoundary, out fvgHighBoundary))
                            {
                                Print($"Bearish FVG found on bar {GetNewYorkTime(contextBars.OpenTimes[fvgSearchIndex]):yyyy-MM-dd HH:mm} NY. Range: {fvgLowBoundary}-{fvgHighBoundary}");
                                _lastFvgDetermined_Low = fvgLowBoundary;
                                _lastFvgDetermined_High = fvgHighBoundary;
                                _fvgBarN_High = contextBars.HighPrices[fvgSearchIndex-1]; 
                                _fvgBarN_Low = contextBars.LowPrices[fvgSearchIndex-1]; 
                                PrepareAndPlaceFVGEntryOrder(SweepType.HighSwept); // Pass only direction
                                fvgFound = true;
                            }
                        }
                        else // Bullish BOS, look for Bullish FVG
                        {
                            if (FindBullishFVG(fvgSearchIndex, contextBars, out fvgLowBoundary, out fvgHighBoundary))
                            {
                                Print($"Bullish FVG found on bar {GetNewYorkTime(contextBars.OpenTimes[fvgSearchIndex]):yyyy-MM-dd HH:mm} NY. Range: {fvgLowBoundary}-{fvgHighBoundary}");
                                _lastFvgDetermined_Low = fvgLowBoundary;
                                _lastFvgDetermined_High = fvgHighBoundary;
                                _fvgBarN_Low = contextBars.LowPrices[fvgSearchIndex-1]; 
                                _fvgBarN_High = contextBars.HighPrices[fvgSearchIndex-1];
                                PrepareAndPlaceFVGEntryOrder(SweepType.LowSwept); // Pass only direction
                                fvgFound = true;
                            }
                        }

                        if (fvgFound)
                        {
                            _lastSweepType = SweepType.None; 
                            _relevantSwingLevelForBOS = 0;
                            _sweepBarOpenTimeNY = DateTime.MinValue;
                            _sweepBarActualHighOnContextTF = 0;
                            _sweepBarActualLowOnContextTF = 0;
                            return; 
                        }
                    }
                    // If loop completes and no FVG found in the range
                    Print($"BOS confirmed at {_bosTimeNY:yyyy-MM-dd HH:mm} NY, but NO FVG found in range from sweep bar to BOS bar. Resetting.");
                    _lastSweepType = SweepType.None; 
                    _relevantSwingLevelForBOS = 0;
                    _sweepBarOpenTimeNY = DateTime.MinValue;
                    _sweepBarActualHighOnContextTF = 0;
                    _sweepBarActualLowOnContextTF = 0;
                    return; // Reset and wait for new setup
                }
            }
            
            // Optional: Timeout for BOS if too many bars pass after sweep without BOS
            if (contextBars.Count - 1 > firstBarToCheckForBOSIndex + 15 && firstBarToCheckForBOSIndex >=0) // e.g., wait 15 M15 bars for BOS
            {
                Print($"No BOS detected within ~{15 * GetTimeFrameInMinutes(_contextTimeFrame)} mins of sweep. Resetting sweep state for {_lastSweepType}. Sweep bar was {_sweepBarOpenTimeNY:HH:mm}");
                _lastSweepType = SweepType.None;
                _relevantSwingLevelForBOS = 0;
                _sweepBarOpenTimeNY = DateTime.MinValue;
                _sweepBarActualHighOnContextTF = 0;
                _sweepBarActualLowOnContextTF = 0;
            }
        }

        private bool FindBullishFVG(int barN1Index, Bars series, out double fvgLow, out double fvgHigh)
        {
            fvgLow = 0; fvgHigh = 0;
            int barNIndex = barN1Index - 1;
            int barN2Index = barN1Index + 1;

            if (barNIndex < 0 || barN2Index >= series.Count)
            {
                return false; 
            }

            if (series.LowPrices[barN2Index] > series.HighPrices[barNIndex])
            {
                fvgLow = series.HighPrices[barNIndex];    
                fvgHigh = series.LowPrices[barN2Index]; 
                return true;
            }
            return false;
        }

        private bool FindBearishFVG(int barN1Index, Bars series, out double fvgLow, out double fvgHigh)
        {
            fvgLow = 0; fvgHigh = 0;
            int barNIndex = barN1Index - 1;
            int barN2Index = barN1Index + 1;

            if (barNIndex < 0 || barN2Index >= series.Count)
            {
                return false; 
            }

            if (series.HighPrices[barN2Index] < series.LowPrices[barNIndex])
            {
                fvgLow = series.HighPrices[barN2Index]; 
                fvgHigh = series.LowPrices[barNIndex];  
                return true;
            }
            return false;
        }

        private double CalculateOrderVolume(double stopLossPips)
        {
            Print($"--- Calculating Order Volume ---");
            Print($"Input StopLossPips: {stopLossPips}");

            if (stopLossPips <= 0.1) 
            {
                Print($"Stop loss ({stopLossPips} pips) is too small. Min SL for volume calc: 0.1 pips. Volume set to 0.");
                return 0;
            }

            double balance = Account.Balance;
            double riskPercent = RiskPercentage / 100.0;
            double riskAmount = balance * riskPercent;
            Print($"Account Balance: {balance}, RiskPercentage: {RiskPercentage}%, RiskAmount: {riskAmount}");

            double pipValue = Symbol.PipValue;
            Print($"Symbol.PipValue: {pipValue}");
            if (pipValue == 0) 
            {
                Print("Error: Symbol.PipValue is zero. Cannot calculate volume. Volume set to 0.");
                return 0; 
            }
            
            double volumeInLotsCalc = riskAmount / (stopLossPips * pipValue); 
            Print($"Calculated Volume (in Lots, direct formula): {volumeInLotsCalc}");

            double normalizedVolume = Symbol.NormalizeVolumeInUnits(volumeInLotsCalc, RoundingMode.Down);
            Print($"Normalized Volume (Lots): {normalizedVolume}");
            Print($"Symbol MinVolume: {Symbol.VolumeInUnitsMin}, MaxVolume: {Symbol.VolumeInUnitsMax}, LotStep: {Symbol.VolumeInUnitsStep}");

            if (normalizedVolume < Symbol.VolumeInUnitsMin)
            {
                Print($"Calculated volume {normalizedVolume} is less than minimum {Symbol.VolumeInUnitsMin}. Volume set to 0.");
                return 0;
            }
            if (Symbol.VolumeInUnitsMax > 0 && normalizedVolume > Symbol.VolumeInUnitsMax) 
            {
                Print($"Calculated volume {normalizedVolume} is greater than maximum {Symbol.VolumeInUnitsMax}. Using max volume: {Symbol.VolumeInUnitsMax}.");
                return Symbol.VolumeInUnitsMax;
            }
            
            Print($"--- Final Calculated Volume (Lots): {normalizedVolume} ---");
            return normalizedVolume;
        }

        private void PrepareAndPlaceFVGEntryOrder(SweepType bosDirection) // Removed fvgMiddleBarIndex and contextBars as FVG details are now global
        {
            if (_sweepBarActualHighOnContextTF == 0 || _sweepBarActualLowOnContextTF == 0)
            {
                Print("Error: Sweep bar High/Low for SL not set. Order not placed.");
                // Reset state to avoid stuck logic
                _lastSweepType = SweepType.None; 
                _relevantSwingLevelForBOS = 0;
                _sweepBarOpenTimeNY = DateTime.MinValue;
                return;
            }

            if (!string.IsNullOrEmpty(_currentPendingOrderLabel))
            {
                var existingOrder = PendingOrders.FirstOrDefault(o => o.Label == _currentPendingOrderLabel);
                if (existingOrder != null)
                {
                    CancelPendingOrderAsync(existingOrder, cr => 
                    {
                        if (cr.IsSuccessful) Print($"Previous pending order {existingOrder.Label} cancelled.");
                        else Print($"Failed to cancel previous pending order {existingOrder.Label}: {cr.Error}");
                    });
                    _currentPendingOrderLabel = null; 
                }
            }

            double entryPrice, stopLossPrice, takeProfitPrice;
            TradeType tradeType;
            string newOrderLabel = $"SB_{SymbolName}_{Server.Time:yyyyMMddHHmmss}";
            double stopLossBufferPips = Symbol.PipSize * 2; // 2 pips buffer

            if (bosDirection == SweepType.LowSwept) // Bullish BOS after LowSweep
            {
                tradeType = TradeType.Buy;
                entryPrice = _lastFvgDetermined_High; // Entry at the top of Bullish FVG (Low of bar N+2 of FVG pattern)
                stopLossPrice = _sweepBarActualLowOnContextTF - stopLossBufferPips; // SL below the low of the SWEEP bar
                Print($"FVG Entry: {_lastFvgDetermined_High}, SL based on Sweep Bar Low: {_sweepBarActualLowOnContextTF}");
            }
            else // Bearish BOS after HighSwept
            {
                tradeType = TradeType.Sell;
                entryPrice = _lastFvgDetermined_Low; // Entry at the bottom of Bearish FVG (High of bar N+2 of FVG pattern)
                stopLossPrice = _sweepBarActualHighOnContextTF + stopLossBufferPips; // SL above the high of the SWEEP bar
                Print($"FVG Entry: {_lastFvgDetermined_Low}, SL based on Sweep Bar High: {_sweepBarActualHighOnContextTF}");
            }

            if ((tradeType == TradeType.Buy && entryPrice <= stopLossPrice) || (tradeType == TradeType.Sell && entryPrice >= stopLossPrice))
            {
                Print($"Invalid SL/Entry: Entry {entryPrice}, SL {stopLossPrice} for {tradeType}. Order not placed.");
                return;
            }
            
            double stopLossInPips = Math.Abs(entryPrice - stopLossPrice) / Symbol.PipSize;
            if (stopLossInPips <= 0.5) 
            {
                Print($"Stop loss is too small ({stopLossInPips} pips based on sweep bar SL). Min SL: 0.5 pips. Order not placed.");
                return;
            }

            double volume = CalculateOrderVolume(stopLossInPips);
            if (volume == 0)
            {
                Print("Calculated volume is zero. Order not placed.");
                return;
            }

            if (tradeType == TradeType.Buy)
            {
                takeProfitPrice = entryPrice + (stopLossInPips * RiskRewardRatio * Symbol.PipSize);
            }
            else
            {
                takeProfitPrice = entryPrice - (stopLossInPips * RiskRewardRatio * Symbol.PipSize);
            }
            
            if (tradeType == TradeType.Buy && entryPrice >= Symbol.Ask)
            {
                Print($"Buy Limit entry {entryPrice} is at or above current Ask {Symbol.Ask}. Consider Market Order or adjust. Order not placed.");
                 return; 
            }
            if (tradeType == TradeType.Sell && entryPrice <= Symbol.Bid)
            {
                Print($"Sell Limit entry {entryPrice} is at or below current Bid {Symbol.Bid}. Consider Market Order or adjust. Order not placed.");
                 return;
            }

            Print($"Preparing to place {tradeType} Limit Order: Label={newOrderLabel}, Vol={volume}, Entry={Math.Round(entryPrice, Symbol.Digits)}, SL={Math.Round(stopLossPrice, Symbol.Digits)} ({stopLossInPips} pips), TP={Math.Round(takeProfitPrice, Symbol.Digits)}");

            // Explicitly cast the 8th argument (null) to ProtectionType? to resolve ambiguity
            var result = PlaceLimitOrderAsync(tradeType, SymbolName, volume, entryPrice, newOrderLabel, stopLossPrice, takeProfitPrice, (ProtectionType?)null, null);
            _currentPendingOrderLabel = newOrderLabel; 
        }

        private void OnPendingOrderFilled(PendingOrderFilledEventArgs args)
        {
            Print($"Pending order {args.PendingOrder.Label} filled and became position {args.Position.Id} (Symbol: {args.Position.SymbolName}, Type: {args.Position.TradeType}, Volume: {args.Position.VolumeInUnits}, Entry: {args.Position.EntryPrice}).");
            if (args.PendingOrder.Label == _currentPendingOrderLabel)
            {
                _currentPendingOrderLabel = null; 
            }
        }

        private void OnPendingOrderCancelled(PendingOrderCancelledEventArgs args)
        {
            Print($"Pending order {args.PendingOrder.Label} was cancelled. Reason: {args.Reason}");
            if (args.PendingOrder.Label == _currentPendingOrderLabel)
            {
                _currentPendingOrderLabel = null;
            }
        }

        private void OnPositionOpened(PositionOpenedEventArgs args)
        { 
            Print($"Position {args.Position.Id} opened (Symbol: {args.Position.SymbolName}, Type: {args.Position.TradeType}, Volume: {args.Position.VolumeInUnits}, Entry: {args.Position.EntryPrice}, SL: {args.Position.StopLoss}, TP: {args.Position.TakeProfit}).");
        }

        private void OnPositionClosed(PositionClosedEventArgs args)
        {
            var position = args.Position;
            // Use LINQ FirstOrDefault on History collection to find the trade by PositionId
            var trade = History.FirstOrDefault(ht => ht.PositionId == position.Id);

            double closingPrice;
            string closingPriceSourceInfo;

            if (trade != null)
            {
                closingPrice = trade.ClosingPrice;
                closingPriceSourceInfo = "History.Trade.ClosingPrice";
            }
            else
            {
                Print($"Warning: Could not find HistoricalTrade for Position ID {position.Id}. Using position.EntryPrice as fallback for ClosingPrice in Pips calculation.");
                closingPrice = position.EntryPrice; // Fallback to avoid errors, Pips calculation will be 0.
                closingPriceSourceInfo = "FallbackToEntryPrice";
            }

            string PnLStatement = $"P&L: {position.GrossProfit} (Net: {position.NetProfit}, Commission: {position.Commissions}, Swap: {position.Swap})";
            string entryExit = $"Entry: {position.EntryPrice}, Exit: {closingPrice} (Source: {closingPriceSourceInfo}), Type: {position.TradeType}";
            
            double pipsLostOrGained = 0;
            if (position.TradeType == TradeType.Buy) {
                pipsLostOrGained = (closingPrice - position.EntryPrice) / Symbol.PipSize;
            } else {
                pipsLostOrGained = (position.EntryPrice - closingPrice) / Symbol.PipSize;
            }
            Print($"Position {position.Id} closed. {entryExit}. Pips: {pipsLostOrGained:F1}. {PnLStatement}. Reason: {args.Reason}");
        }

        // Helper method to get TimeFrame duration in minutes
        private int GetTimeFrameInMinutes(TimeFrame timeFrame)
        {
            if (timeFrame == TimeFrame.Minute) return 1;
            if (timeFrame == TimeFrame.Minute5) return 5;
            if (timeFrame == TimeFrame.Minute15) return 15;
            if (timeFrame == TimeFrame.Minute30) return 30;
            if (timeFrame == TimeFrame.Hour) return 60;
            if (timeFrame == TimeFrame.Hour4) return 240;
            if (timeFrame == TimeFrame.Daily) return 1440;
            
            // Fallback for unhandled timeframes - you might want to log or throw an error
            Print($"Warning: GetTimeFrameInMinutes does not have a specific case for {timeFrame}. Defaulting to 15 minutes.");
            return 15; 
        }
    }
}
