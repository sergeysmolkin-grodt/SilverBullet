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
        [Parameter("Session 1 Start (NY)", DefaultValue = "03:00")]
        public string Session1StartNY { get; set; }

        [Parameter("Session 1 End (NY)", DefaultValue = "04:00")]
        public string Session1EndNY { get; set; }

        [Parameter("Session 2 Start (NY)", DefaultValue = "10:00")]
        public string Session2StartNY { get; set; }

        [Parameter("Session 2 End (NY)", DefaultValue = "11:00")]
        public string Session2EndNY { get; set; }

        [Parameter("Session 3 Start (NY)", DefaultValue = "14:00")]
        public string Session3StartNY { get; set; }

        [Parameter("Session 3 End (NY)", DefaultValue = "15:00")]
        public string Session3EndNY { get; set; }

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
        private DateTime _timeOfLastSweepNY; // Precise time of sweep
        private DateTime _sweepBarTimeNY; // Open time of the bar where sweep occurred (NY)

        private double _relevantSwingLevelForBOS;
        private DateTime _relevantSwingBarTimeNY; // Open time of the identified swing bar (NY)

        private double _bosLevel;
        private DateTime _bosTimeNY;

        private string _currentPendingOrderLabel; // To store the label of the active pending order
        private double _lastFvgDetermined_Low;    // Stores Low of the identified FVG range
        private double _lastFvgDetermined_High;   // Stores High of the identified FVG range
        private double _fvgBarN_Low;              // Stores Low of the N bar (first bar of FVG pattern) for SL
        private double _fvgBarN_High;             // Stores High of the N bar (first bar of FVG pattern) for SL

        private struct SessionInfo
        {
            public bool IsActivePeriod; // True if current time is within [SessionStart-Buffer, SessionEnd)
            public DateTime ActualStartNY; // Actual start time of the session (e.g., 03:00:00)
            public DateTime ActualEndNY;   // Actual end time of the session (e.g., 04:00:00)
            public int SessionNumber;    // 1, 2, or 3. 0 if not in any session's active period.
        }

        protected override void OnStart()
        {
            // Initialize New York TimeZoneInfo
            try
            {
                _newYorkTimeZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
                if (_newYorkTimeZone == null) // Fallback for systems where "Eastern Standard Time" might not be available or named differently
                {
                    _newYorkTimeZone = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
                }
            }
            catch (TimeZoneNotFoundException)
            {
                Print("New York Time Zone not found. Please check TimeZone ID. Bot will use UTC.");
                _newYorkTimeZone = TimeZoneInfo.Utc; // Default to UTC if NY time zone is not found
            }
            catch (Exception ex)
            {
                Print($"Error initializing New York Time Zone: {ex.Message}. Bot will use UTC.");
                _newYorkTimeZone = TimeZoneInfo.Utc;
            }

            // Parse ContextTimeFrameString
            try
            {
                _contextTimeFrame = TimeFrame.Parse(ContextTimeFrameString);
            }
            catch (ArgumentException)
            {
                Print($"Error parsing ContextTimeFrameString: '{ContextTimeFrameString}'. Defaulting to m15.");
                _contextTimeFrame = TimeFrame.Minute15;
            }

            Print("Silver Bullet Bot Started.");
            Print($"Trading Sessions (NY Time): {Session1StartNY}-{Session1EndNY}, {Session2StartNY}-{Session2EndNY}, {Session3StartNY}-{Session3EndNY}");
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

                if (_relevantSwingLevelForBOS == 0) // Swing for BOS not yet identified
                {
                    IdentifyRelevantSwingForBOS(contextBars);
                }
                
                if (_relevantSwingLevelForBOS != 0) // If swing identified, check for BOS
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
                // If outside active session and a sweep was being monitored, reset it if BOS didn't form.
                // This might need refinement based on how long we wait for BOS.
                if (_lastSweepType != SweepType.None)
                {
                    // Print($"Outside active session, resetting sweep type {_lastSweepType}");
                    // _lastSweepType = SweepType.None; // Potentially reset here or based on a timeout for BOS
                }
            }
        }

        protected override void OnStop()
        {
            Print("Silver Bullet Bot Stopped.");
            // Optionally, cancel any pending orders on stop
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
            // Ensure serverTime is UTC before conversion for robustness
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

                _session1ActualStartNY = currentTimeNY.Date + TimeSpan.Parse(Session1StartNY);
                _session1ActualEndNY = currentTimeNY.Date + TimeSpan.Parse(Session1EndNY);
                _session2ActualStartNY = currentTimeNY.Date + TimeSpan.Parse(Session2StartNY);
                _session2ActualEndNY = currentTimeNY.Date + TimeSpan.Parse(Session2EndNY);
                _session3ActualStartNY = currentTimeNY.Date + TimeSpan.Parse(Session3StartNY);
                _session3ActualEndNY = currentTimeNY.Date + TimeSpan.Parse(Session3EndNY);

                _liquidityIdentifiedForSession1 = false;
                _liquidityIdentifiedForSession2 = false;
                _liquidityIdentifiedForSession3 = false;
                _currentLiquidityHigh = 0;
                _currentLiquidityLow = 0;
                // Print("Liquidity flags reset for the new day.");
            }
        }

        private SessionInfo GetCurrentSessionInfo(DateTime currentTimeNY)
        {
            TimeSpan currentTimeOfDay = currentTimeNY.TimeOfDay;
            TimeSpan preBuffer = TimeSpan.FromMinutes(MinutesBeforeSessionForLiquidity);

            // Session 1 Check
            if (currentTimeNY >= _session1ActualStartNY.Subtract(preBuffer) && currentTimeNY < _session1ActualEndNY)
            {
                return new SessionInfo { IsActivePeriod = true, ActualStartNY = _session1ActualStartNY, ActualEndNY = _session1ActualEndNY, SessionNumber = 1 };
            }
            // Session 2 Check
            if (currentTimeNY >= _session2ActualStartNY.Subtract(preBuffer) && currentTimeNY < _session2ActualEndNY)
            {
                return new SessionInfo { IsActivePeriod = true, ActualStartNY = _session2ActualStartNY, ActualEndNY = _session2ActualEndNY, SessionNumber = 2 };
            }
            // Session 3 Check
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

            // We need the bar that closed *just before* sessionActualStartNY.
            // Its OpenTime will be <= sessionActualStartNY.AddTicks(-1) (NY time).
            DateTime timeForLiquidityBarTargetNY = sessionActualStartNY.AddTicks(-1);
            DateTime timeForLiquidityBarTargetUtc;

            try
            {
                // Ensure timeForLiquidityBarTargetNY is treated as NY time for conversion
                // If Kind is Unspecified, ConvertTime assumes it's 'sourceZone.Kind' which might not be what we want.
                // Explicitly create a DateTime that IS in New York time, then convert.
                DateTime nyDateTimeToConvert;
                if (timeForLiquidityBarTargetNY.Kind == DateTimeKind.Utc)
                { // This should not happen if logic is correct, but as a safeguard
                    nyDateTimeToConvert = TimeZoneInfo.ConvertTimeFromUtc(timeForLiquidityBarTargetNY, _newYorkTimeZone);
                }
                else if (timeForLiquidityBarTargetNY.Kind == DateTimeKind.Local)
                { // If local time is NY, this is fine. If not, it's wrong. Better to build from components.
                    // For simplicity, assuming GetNewYorkTime and sessionActualStartNY setup is correct,
                    // timeForLiquidityBarTargetNY should represent a NY moment.
                    nyDateTimeToConvert = timeForLiquidityBarTargetNY; 
                }
                else // Unspecified
                {
                    nyDateTimeToConvert = timeForLiquidityBarTargetNY; // Assume it represents NY time as per calculation
                }

                timeForLiquidityBarTargetUtc = TimeZoneInfo.ConvertTime(nyDateTimeToConvert, _newYorkTimeZone, TimeZoneInfo.Utc);
            }
            catch (Exception ex)
            {
                Print($"Error converting liquidity target time to UTC for session {sessionNumber}: {ex.Message}. Original NY time: {timeForLiquidityBarTargetNY}, Kind: {timeForLiquidityBarTargetNY.Kind}");
                return;
            }

            // TimeSeries.GetIndexByTime(DateTime) does not take a SearchMode parameter.
            // It should find the index of the bar active at the given time, or the one immediately preceding if not an exact match.
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
            // Ensure there's valid liquidity to check against
            // And ensure we haven't already detected a sweep that is pending BOS processing
            if ((_currentLiquidityHigh == 0 && _currentLiquidityLow == 0) || _lastSweepType != SweepType.None) return;

            double currentAsk = Symbol.Ask;
            double currentBid = Symbol.Bid;

            // bool highSwept = false; // Will be determined by setting _lastSweepType
            // bool lowSwept = false;

            if (_currentLiquidityHigh != 0 && currentAsk > _currentLiquidityHigh)
            {
                Print($"HIGH LIQUIDITY SWEPT at {_currentLiquidityHigh}. Price: {currentAsk}, Time: {currentTimeNY:HH:mm:ss} NY");
                _lastSweepType = SweepType.HighSwept;
                _lastSweptLiquidityLevel = _currentLiquidityHigh;
                _timeOfLastSweepNY = currentTimeNY;
                _currentLiquidityHigh = 0; // Mark as swept for this session's liquidity identification phase
                _currentLiquidityLow = 0; // Also clear low, as we focus on one sweep at a time
            }
            else if (_currentLiquidityLow != 0 && currentBid < _currentLiquidityLow) // Use else if to prioritize one sweep per tick if somehow both happen
            {
                Print($"LOW LIQUIDITY SWEPT at {_currentLiquidityLow}. Price: {currentBid}, Time: {currentTimeNY:HH:mm:ss} NY");
                _lastSweepType = SweepType.LowSwept;
                _lastSweptLiquidityLevel = _currentLiquidityLow;
                _timeOfLastSweepNY = currentTimeNY;
                _currentLiquidityLow = 0; // Mark as swept for this session's liquidity identification phase
                _currentLiquidityHigh = 0; // Also clear high
            }

            if (_lastSweepType != SweepType.None)
            {
                Print($"Liquidity sweep detected ({_lastSweepType.ToString()}). Level: {_lastSweptLiquidityLevel}. Time: {_timeOfLastSweepNY:HH:mm:ss} NY. Identifying relevant swing for BOS.");
                
                // Get context bars once for identifying the swing
                var contextBars = MarketData.GetBars(_contextTimeFrame);
                if (contextBars.Count > 0)
                {
                    IdentifyRelevantSwingForBOS(contextBars); 
                }
                else
                {
                    Print("Cannot identify swing for BOS: Context bars are empty.");
                    _lastSweepType = SweepType.None; // Reset if we can't get bars
                }
            }
        }

        private bool IsSwingHigh(int barIndex, Bars series, int swingCandles = 1)
        {
            if (barIndex < swingCandles || barIndex >= series.Count - swingCandles)
                return false; // Not enough bars on either side

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
                return false; // Not enough bars on either side

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
            if (_lastSweepType == SweepType.None || contextBars.Count == 0)
            {
                return;
            }

            DateTime sweepTimeUtc = TimeZoneInfo.ConvertTime(_timeOfLastSweepNY, _newYorkTimeZone, TimeZoneInfo.Utc);
            int sweepBarIndex = contextBars.OpenTimes.GetIndexByTime(sweepTimeUtc); 
            if (sweepBarIndex < 0) sweepBarIndex = contextBars.Count -1; // If exact time not found, assume last bar or handle error
            
            _sweepBarTimeNY = GetNewYorkTime(contextBars.OpenTimes[sweepBarIndex]); // Store the open time of the sweep bar

            _relevantSwingLevelForBOS = 0; // Reset before search
            _relevantSwingBarTimeNY = DateTime.MinValue;

            // Look for swing point before the sweep bar
            int searchStartIndex = sweepBarIndex - 1;
            int searchEndIndex = Math.Max(0, sweepBarIndex - SwingLookbackPeriod);

            if (_lastSweepType == SweepType.HighSwept) // Swept high, looking for a swing LOW to break (bearish BOS)
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
                Print("No relevant Swing Low found within lookback period for BOS after High Sweep.");
            }
            else if (_lastSweepType == SweepType.LowSwept) // Swept low, looking for a swing HIGH to break (bullish BOS)
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
                Print("No relevant Swing High found within lookback period for BOS after Low Sweep.");
            }
            
            // If no swing found, reset sweep state to allow new liquidity detection
            if(_relevantSwingLevelForBOS == 0)
            {
                _lastSweepType = SweepType.None;
            }
        }

        private void CheckForBOS(DateTime currentTimeNY, Bars contextBars)
        {
            if (_lastSweepType == SweepType.None || _relevantSwingLevelForBOS == 0 || contextBars.Count == 0)
            {
                return;
            }

            // Determine the bar index where the sweep happened to start checking for BOS from the next bar.
            DateTime sweepBarTimeUtc = TimeZoneInfo.ConvertTime(_sweepBarTimeNY, _newYorkTimeZone, TimeZoneInfo.Utc);
            int sweepBarIndex = contextBars.OpenTimes.GetIndexByTime(sweepBarTimeUtc);
            if (sweepBarIndex < 0) sweepBarIndex = contextBars.Count - 2; // Fallback, should be rare if _sweepBarTimeNY is valid.
            
            // Check bars from the one after the sweep bar up to the latest fully closed bar
            // contextBars.Count - 1 is the current, forming bar. We check up to contextBars.Count - 2.
            for (int i = sweepBarIndex + 1; i < contextBars.Count -1; i++) // Iterate up to second to last bar (last closed bar)
            {
                if (GetNewYorkTime(contextBars.OpenTimes[i]) <= _relevantSwingBarTimeNY) // Don't check BOS on or before the swing itself
                    continue;

                if (_lastSweepType == SweepType.HighSwept) // Bearish BOS: Close below the identified Swing Low level
                {
                    if (contextBars.ClosePrices[i] < _relevantSwingLevelForBOS)
                    {
                        _bosLevel = _relevantSwingLevelForBOS; // Or more precisely, the close price contextBars.ClosePrices[i]
                        _bosTimeNY = GetNewYorkTime(contextBars.OpenTimes[i]);
                        Print($"BEARISH BOS DETECTED at bar {_bosTimeNY:yyyy-MM-dd HH:mm} NY. Structure broken below Swing Low: {Math.Round(_relevantSwingLevelForBOS, Symbol.Digits)}. Sweep was: {_lastSweepType}");
                        
                        // Check for Imbalance (FVG)
                        double fvgLowBoundary, fvgHighBoundary; // These are the narrow FVG levels
                        if (FindBearishFVG(i, contextBars, out fvgLowBoundary, out fvgHighBoundary))
                        {
                            Print($"Bearish FVG confirmed with BOS. FVG Range for entry test: {Math.Round(fvgLowBoundary, Symbol.Digits)} - {Math.Round(fvgHighBoundary, Symbol.Digits)}.");
                            _lastFvgDetermined_Low = fvgLowBoundary;
                            _lastFvgDetermined_High = fvgHighBoundary;
                            _fvgBarN_High = contextBars.HighPrices[i-1]; // High of bar N for SL
                            _fvgBarN_Low = contextBars.LowPrices[i-1]; // Low of bar N (not strictly needed for Bearish SL but good to have)
                            PrepareAndPlaceFVGEntryOrder(SweepType.HighSwept, i, contextBars);
                        }
                        else
                        {
                            Print("Bearish BOS detected, but no FVG found on the BOS bar.");
                        }

                        _lastSweepType = SweepType.None; // Reset for new cycle
                        _relevantSwingLevelForBOS = 0;
                        return; 
                    }
                }
                else if (_lastSweepType == SweepType.LowSwept) // Bullish BOS: Close above the identified Swing High level
                {
                    if (contextBars.ClosePrices[i] > _relevantSwingLevelForBOS)
                    {
                        _bosLevel = _relevantSwingLevelForBOS;
                        _bosTimeNY = GetNewYorkTime(contextBars.OpenTimes[i]);
                        Print($"BULLISH BOS DETECTED at bar {_bosTimeNY:yyyy-MM-dd HH:mm} NY. Structure broken above Swing High: {Math.Round(_relevantSwingLevelForBOS, Symbol.Digits)}. Sweep was: {_lastSweepType}");

                        // Check for Imbalance (FVG)
                        double fvgLowBoundary, fvgHighBoundary;
                        if (FindBullishFVG(i, contextBars, out fvgLowBoundary, out fvgHighBoundary))
                        {
                            Print($"Bullish FVG confirmed with BOS. FVG Range for entry test: {Math.Round(fvgLowBoundary, Symbol.Digits)} - {Math.Round(fvgHighBoundary, Symbol.Digits)}.");
                            _lastFvgDetermined_Low = fvgLowBoundary;
                            _lastFvgDetermined_High = fvgHighBoundary;
                            _fvgBarN_Low = contextBars.LowPrices[i-1]; // Low of bar N for SL
                            _fvgBarN_High = contextBars.HighPrices[i-1]; // High of bar N (not strictly needed for Bullish SL)
                            PrepareAndPlaceFVGEntryOrder(SweepType.LowSwept, i, contextBars);
                        }
                        else
                        {
                            Print("Bullish BOS detected, but no FVG found on the BOS bar.");
                        }

                        _lastSweepType = SweepType.None; // Reset for new cycle
                        _relevantSwingLevelForBOS = 0;
                        return;
                    }
                }
            }
            
            // Optional: Timeout for BOS - if too many bars pass after sweep without BOS, reset _lastSweepType
            // For example, if current bar index is sweepBarIndex + SomeMaxBarsToWaitForBOS
            // if (contextBars.Count - 1 > sweepBarIndex + 15 && sweepBarIndex >=0) // e.g., wait 15 bars for BOS
            // {
            //     Print($"No BOS detected within 15 bars of sweep. Resetting sweep state for {_lastSweepType}.");
            //     _lastSweepType = SweepType.None;
            //     _relevantSwingLevelForBOS = 0;
            // }
        }

        private bool FindBullishFVG(int barN1Index, Bars series, out double fvgLow, out double fvgHigh)
        {
            fvgLow = 0; fvgHigh = 0;
            // N is barN1Index - 1, N+1 is barN1Index, N+2 is barN1Index + 1
            int barNIndex = barN1Index - 1;
            int barN2Index = barN1Index + 1;

            if (barNIndex < 0 || barN2Index >= series.Count)
            {
                return false; // Not enough bars for a 3-bar pattern
            }

            // Bullish FVG: Low of N+2 bar is above High of N bar
            if (series.LowPrices[barN2Index] > series.HighPrices[barNIndex])
            {
                fvgLow = series.HighPrices[barNIndex];    // Top of the gap (High of bar N)
                fvgHigh = series.LowPrices[barN2Index]; // Bottom of the gap (Low of bar N+2)
                return true;
            }
            return false;
        }

        private bool FindBearishFVG(int barN1Index, Bars series, out double fvgLow, out double fvgHigh)
        {
            fvgLow = 0; fvgHigh = 0;
            // N is barN1Index - 1, N+1 is barN1Index, N+2 is barN1Index + 1
            int barNIndex = barN1Index - 1;
            int barN2Index = barN1Index + 1;

            if (barNIndex < 0 || barN2Index >= series.Count)
            {
                return false; // Not enough bars for a 3-bar pattern
            }

            // Bearish FVG: High of N+2 bar is below Low of N bar
            if (series.HighPrices[barN2Index] < series.LowPrices[barNIndex])
            {
                fvgLow = series.HighPrices[barN2Index]; // Top of the gap (High of bar N+2)
                fvgHigh = series.LowPrices[barNIndex];  // Bottom of the gap (Low of bar N)
                return true;
            }
            return false;
        }

        private double CalculateOrderVolume(double stopLossPips)
        {
            if (stopLossPips <= 0) return 0;

            double riskAmount = Account.Balance * (RiskPercentage / 100.0);
            double pipValue = Symbol.PipValue;
            if (pipValue == 0) 
            { 
                Print("Error: Symbol.PipValue is zero. Cannot calculate volume.");
                return 0; 
            }
            
            // Quantity in base currency units
            double quantity = riskAmount / (stopLossPips * pipValue);
            
            // Convert quantity to volume in lots
            double volumeInLots = Symbol.QuantityToVolumeInUnits(quantity);

            // Normalize the volume to the symbol's lot step
            volumeInLots = Symbol.NormalizeVolumeInUnits(volumeInLots, RoundingMode.Down);
            
            if (volumeInLots < Symbol.VolumeInUnitsMin)
            {
                Print($"Calculated volume {volumeInLots} is less than minimum {Symbol.VolumeInUnitsMin}. No trade placed.");
                return 0;
            }
            if (volumeInLots > Symbol.VolumeInUnitsMax && Symbol.VolumeInUnitsMax > 0)
            {
                Print($"Calculated volume {volumeInLots} is greater than maximum {Symbol.VolumeInUnitsMax}. Using max volume.");
                return Symbol.VolumeInUnitsMax;
            }
            return volumeInLots;
        }

        private void PrepareAndPlaceFVGEntryOrder(SweepType bosDirection, int fvgMiddleBarIndex, Bars contextBars)
        {
            // 1. Cancel existing pending order if any
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
                    _currentPendingOrderLabel = null; // Clear it assuming cancellation will succeed or it becomes irrelevant
                }
            }

            double entryPrice, stopLossPrice, takeProfitPrice;
            TradeType tradeType;
            string newOrderLabel = $"SB_{SymbolName}_{Server.Time:yyyyMMddHHmmss}";

            double stopLossBufferPips = Symbol.PipSize * 2; // 2 pips buffer for SL

            if (bosDirection == SweepType.LowSwept) // Bullish BOS, means we had a LowSweep, expecting Bullish FVG
            {
                tradeType = TradeType.Buy;
                entryPrice = _lastFvgDetermined_High; // Entry at the top of Bullish FVG (Low of bar N+2)
                stopLossPrice = _fvgBarN_Low - stopLossBufferPips; // SL below the low of Bar N (1st bar of FVG pattern)
            }
            else // Bearish BOS (HighSwept), expecting Bearish FVG
            {
                tradeType = TradeType.Sell;
                entryPrice = _lastFvgDetermined_Low; // Entry at the bottom of Bearish FVG (High of bar N+2)
                stopLossPrice = _fvgBarN_High + stopLossBufferPips; // SL above the high of Bar N
            }

            double stopLossInPips = Math.Abs(entryPrice - stopLossPrice) / Symbol.PipSize;
            if (stopLossInPips <= 0.5) // Minimum SL of 0.5 pips to avoid issues
            {
                Print($"Stop loss is too small ({stopLossInPips} pips). Min SL: 0.5 pips. Order not placed.");
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
            
            // Ensure entry price makes sense relative to current market for limit orders
            if (tradeType == TradeType.Buy && entryPrice >= Symbol.Ask)
            {
                Print($"Buy Limit entry {entryPrice} is at or above current Ask {Symbol.Ask}. Consider Market Order or adjust. Order not placed.");
                // return; // Or place market order if strategy allows, for now, just skip
            }
            if (tradeType == TradeType.Sell && entryPrice <= Symbol.Bid)
            {
                Print($"Sell Limit entry {entryPrice} is at or below current Bid {Symbol.Bid}. Consider Market Order or adjust. Order not placed.");
                // return;
            }

            Print($"Preparing to place {tradeType} Limit Order: Label={newOrderLabel}, Vol={volume}, Entry={Math.Round(entryPrice, Symbol.Digits)}, SL={Math.Round(stopLossPrice, Symbol.Digits)} ({stopLossInPips} pips), TP={Math.Round(takeProfitPrice, Symbol.Digits)}");

            var result = PlaceLimitOrderAsync(tradeType, SymbolName, volume, entryPrice, newOrderLabel, stopLossPrice, takeProfitPrice);
            // Handling the result of async operation can be done via event or by checking task status if needed, but for now, we fire and forget.
            // If successful, OnPendingOrderCreated might be useful if we need immediate confirmation.
            // For now, we'll rely on OnPendingOrderFilled/Cancelled for state changes.
            _currentPendingOrderLabel = newOrderLabel; // Assume placement attempt, update on events
        }

        private void OnPendingOrderFilled(PendingOrderFilledEventArgs args)
        {
            Print($"Pending order {args.PendingOrder.Label} filled and became position {args.Position.Label}.");
            if (args.PendingOrder.Label == _currentPendingOrderLabel)
            {
                _currentPendingOrderLabel = null; 
            }
            // Potentially set _activeTradeLabel = args.Position.Label; if needed for trade management
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
            Print($"Position {args.Position.Label} opened.");
            // If opened directly, not via our pending order, this might be relevant.
        }

        private void OnPositionClosed(PositionClosedEventArgs args)
        {
            Print($"Position {args.Position.Label} closed. P&L: {args.Position.GrossProfit}");
            // Manage _activeTradeLabel if used
        }
    }
}
