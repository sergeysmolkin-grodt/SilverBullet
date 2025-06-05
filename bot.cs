using System;
using System.Linq;
using cAlgo.API;
using cAlgo.API.Indicators;
using cAlgo.API.Internals;
using cAlgo.Indicators;

namespace cAlgo.Robots
{
    // Enum to represent the type of liquidity sweep
    public enum SweepType
    {
        None,
        HighSwept,
        LowSwept
    }

    public enum TrendContext
    {
        None,
        Bullish,
        Bearish
    }

    [Robot(TimeZone = TimeZones.UTC, AccessRights = AccessRights.None)]
    public class SilverBulletBot : Robot
    {
        // Chart Object Names
        private const string LiqHighLineName = "SB_LiquidityHighLine";
        private const string LiqLowLineName = "SB_LiquidityLowLine";
        private const string BosSwingLineName = "SB_BOS_SwingLevelLine";
        private const string FvgRectName = "SB_FVG_Rectangle";
        private const string PendingEntryLineName = "SB_PendingEntryLine";
        private const string PendingSlLineName = "SB_PendingSlLine";
        private const string PendingTpLineName = "SB_PendingTpLine";
        private const string M3BosConfirmationLineName = "SB_M3_BOS_Confirmation_Line";

        // Strategy Constants
        private const double RiskPercentage = 1.0;
        private const double RiskRewardRatio = 1.5;
        private const string ContextTimeFrameString = "m15";
        private const string ExecutionTimeFrameString = "m3";
        private const string ContextTimeFrameForTrendString = "m15";

        // Daily Trading Limits
        private int _tradesTakenToday = 0;
        private bool _dailyProfitTargetMet = false;

        [Parameter("Minutes Before Session for Liquidity", DefaultValue = 10, MinValue = 1, Group = "Session Timing")]
        public int MinutesBeforeSessionForLiquidity { get; set; }

        [Parameter("Swing Lookback Period", DefaultValue = 20, MinValue = 3, Group = "Strategy Parameters")]
        public int SwingLookbackPeriod { get; set; }

        [Parameter("Swing Candles", DefaultValue = 1, MinValue = 1, MaxValue = 3, Group = "Strategy Parameters")]
        public int SwingCandles { get; set; }

        // Context Parameters
        [Parameter("Trend Lookback Candles", DefaultValue = 30, MinValue = 5, Group = "Context")]
        public int ContextLookbackCandles { get; set; }

        [Parameter("Trend Threshold", DefaultValue = 7, MinValue = 1, Group = "Context")]
        public int ContextTrendThreshold { get; set; }

        // New York TimeZoneInfo
        private TimeZoneInfo _newYorkTimeZone;
        private TimeFrame _contextTimeFrame;
        private TimeFrame _executionTimeFrame;

        // Session timing and liquidity state
        private DateTime _session1ActualStartNY, _session2ActualStartNY, _session3ActualStartNY;
        private DateTime _session1ActualEndNY, _session2ActualEndNY, _session3ActualEndNY;
        private bool _liquidityIdentifiedForSession1, _liquidityIdentifiedForSession2, _liquidityIdentifiedForSession3;
        
        private double _currentLiquidityHigh;
        private double _currentLiquidityLow;
        private DateTime _currentLiquiditySourceBarTimeNY;
        private DateTime _lastDateProcessedForSessionTimes = DateTime.MinValue;

        private SweepType _lastSweepType = SweepType.None;
        private double _lastSweptLiquidityLevel;
        private DateTime _timeOfLastSweepNY; // Precise time of sweep (tick time)
        
        // Sweep Bar details on Context TimeFrame
        private DateTime _sweepBarOpenTimeNY = DateTime.MinValue; // Open time of the M15 bar that swept liquidity
        private double _sweepBarActualHighOnContextTF = 0; // Actual high of the M15 sweep bar
        private double _sweepBarActualLowOnContextTF = 0;  // Actual low of the M15 sweep bar

        private double _relevantSwingLevelForBOS = 0; // The specific high/low of the M3 swing to be broken for BOS
        private DateTime _relevantSwingBarTimeNY = DateTime.MinValue; // NYTime of the M3 bar that formed the _relevantSwingLevelForBOS
        private double _bosLevel = 0; // Level at which BOS was confirmed
        private DateTime _bosTimeNY = DateTime.MinValue; // Time BOS was confirmed (NY) on execution TF
        private DateTime _fvgFoundOnBarOpenTimeNY = DateTime.MinValue; // NY Time of the M3 bar on which FVG was found
        private double _lastFvgDetermined_High = 0;
        private double _lastFvgDetermined_Low = 0;
        private DateTime _fvgBarN_OpenTimeNY = DateTime.MinValue; // FVG Bar N (first bar of 3-bar FVG pattern) OpenTime NY
        private DateTime _fvgBarN2_OpenTimeNY = DateTime.MinValue; // FVG Bar N+2 (third bar of 3-bar FVG pattern) OpenTime NY
        private string _currentPendingOrderLabel = null;
        private DateTime _lastDailyResetTimeNY = DateTime.MinValue;

        // Context Trend Fields
        private TimeFrame _contextTimeFrameForTrend;
        private TrendContext _currentTrendContext = TrendContext.None;
        private DateTime _lastContextTrendCalculationTimeNY = DateTime.MinValue;

        private TimeSpan _contextTimeFrameTimeSpan; // To help with FVG rect drawing duration

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
                Print($"Error parsing ContextTimeFrameString: '{ContextTimeFrameString}'. Defaulting to m15.");
                _contextTimeFrame = TimeFrame.Minute15;
            }
            _contextTimeFrameTimeSpan = GetTimeFrameTimeSpan(_contextTimeFrame); // Initialize TimeSpan for context timeframe

            try
            {
                _executionTimeFrame = TimeFrame.Parse(ExecutionTimeFrameString);
            }
            catch (ArgumentException)
            {
                Print($"Error parsing ExecutionTimeFrameString: '{ExecutionTimeFrameString}'. Defaulting to m3.");
                _executionTimeFrame = TimeFrame.Minute3;
            }

            try
            {
                _contextTimeFrameForTrend = ParseTimeFrame(ContextTimeFrameForTrendString);
            }
            catch (ArgumentException)
            {
                Print($"Error parsing ContextTimeFrameForTrendString: '{ContextTimeFrameForTrendString}'. Defaulting to m15.");
                _contextTimeFrameForTrend = TimeFrame.Minute15;
            }
            Print($"Trend Context TimeFrame: {_contextTimeFrameForTrend}");
            Print($"Trend Lookback Candles: {ContextLookbackCandles}, Trend Threshold: {ContextTrendThreshold}");

            Print("Silver Bullet Bot Started.");
            Print("Trading Sessions (NY Time): 03:00-04:00, 10:00-11:00, 14:00-15:00");
            Print($"Liquidity lookback: {MinutesBeforeSessionForLiquidity} minutes before session start.");
            Print($"Risk per trade: {RiskPercentage}%, RR: {RiskRewardRatio}");
            Print($"Context TimeFrame: {_contextTimeFrame}");
            Print($"Execution TimeFrame: {_executionTimeFrame}");
            Print($"Swing Lookback: {SwingLookbackPeriod} bars, Swing Candles: {SwingCandles}");

            PendingOrders.Filled += OnPendingOrderFilled;
            PendingOrders.Cancelled += OnPendingOrderCancelled;
            Positions.Opened += OnPositionOpened;
            Positions.Closed += OnPositionClosed;

            ClearAllStrategyDrawings(); // Clear any lingering drawings from a previous run
        }

        protected override void OnTick()
        {
            var currentTimeNY = GetNewYorkTime(Server.Time);

            DailyResetAndSessionTimeUpdate(currentTimeNY);
            UpdateTrendContext(currentTimeNY);

            // Visualize M3 sweeps (if enabled and during session)
            // VisualizeM3Sweeps();

            SessionInfo currentSession = GetCurrentSessionInfo(currentTimeNY);

            if (_lastSweepType != SweepType.None)
            {
                if (_relevantSwingLevelForBOS == 0) 
                {
                    IdentifyRelevantM3SwingForBOS(_sweepBarOpenTimeNY, _lastSweepType);
                }
                
                if (_relevantSwingLevelForBOS != 0) 
                {
                    CheckForBOSAndFVG(currentTimeNY);
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
                     // This condition is handled inside IdentifyRelevantM3SwingForBOS now
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

            ClearAllStrategyDrawings();
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
                ClearAllStrategyDrawings(); // Clear drawings for the new day

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

                // Reset daily limits
                _tradesTakenToday = 0;
                _dailyProfitTargetMet = false;
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
                DrawLiquidityLevels(_currentLiquidityHigh, _currentLiquidityLow);
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
                        IdentifyRelevantM3SwingForBOS(_sweepBarOpenTimeNY, _lastSweepType); 
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

            if (_lastSweepType != SweepType.None && _relevantSwingLevelForBOS == 0) // Only identify BOS structure if not already found
            {
                IdentifyRelevantM3SwingForBOS(_sweepBarOpenTimeNY, _lastSweepType);
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

        private void IdentifyRelevantM3SwingForBOS(DateTime m15SweepBarOpenTimeNY, SweepType m15LastSweepType)
        {
            var executionBars = MarketData.GetBars(_executionTimeFrame);
            if (executionBars.Count == 0) return;

            // Convert M15 sweep bar open time from NY to Server time for GetIndexByTime
            DateTime m15SweepBarOpenTimeServer;
            try
            {
                m15SweepBarOpenTimeServer = TimeZoneInfo.ConvertTime(m15SweepBarOpenTimeNY, _newYorkTimeZone, TimeZone);
            }
            catch (ArgumentException ex)
            {
                Print($"Error converting M15 sweep bar time for GetIndexByTime: {ex.Message}. NYTime: {m15SweepBarOpenTimeNY}, TargetZone: {TimeZone.DisplayName}");
                return;
            }
            
            int m3ReferenceBarIndex = executionBars.OpenTimes.GetIndexByTime(m15SweepBarOpenTimeServer);

            if (m3ReferenceBarIndex < 0)
            {
                Print($"Error: Could not find an M3 bar corresponding to M15 sweep bar open time: {m15SweepBarOpenTimeNY:yyyy-MM-dd HH:mm} NY (Server: {m15SweepBarOpenTimeServer:yyyy-MM-dd HH:mm})");
                // If we can't find the bar, it's a critical issue for this path, reset sweep state.
                _lastSweepType = SweepType.None;
                _relevantSwingLevelForBOS = 0;
                _relevantSwingBarTimeNY = DateTime.MinValue;
                _sweepBarOpenTimeNY = DateTime.MinValue;
                _sweepBarActualHighOnContextTF = 0;
                _sweepBarActualLowOnContextTF = 0;
                ClearBosAndFvgDrawings();
                Chart.RemoveObject(M3BosConfirmationLineName);
                return;
            }

            // Define the loop range for searching M3 swings before the m3ReferenceBarIndex
            int loopEndIndex = Math.Max(SwingCandles, m3ReferenceBarIndex - SwingCandles);
            int loopStartIndex = Math.Max(SwingCandles, m3ReferenceBarIndex - SwingLookbackPeriod);


            if (m15LastSweepType == SweepType.LowSwept) // After M15 Low sweep, look for an M3 Swing High to break (Bullish BOS)
            {
                for (int i = loopEndIndex; i >= loopStartIndex; i--)
                {
                    if (i < SwingCandles || i >= executionBars.ClosePrices.Count - SwingCandles) continue; 

                    if (IsSwingHigh(i, executionBars, SwingCandles))
                    {
                        _relevantSwingLevelForBOS = executionBars.HighPrices[i];
                        _relevantSwingBarTimeNY = GetNewYorkTime(executionBars.OpenTimes[i]);
                        Print($"Relevant M3 Swing High for BOS identified at {Math.Round(_relevantSwingLevelForBOS, Symbol.Digits)} (Bar: {_relevantSwingBarTimeNY:yyyy-MM-dd HH:mm} NY on {_executionTimeFrame}) after M15 Low Sweep.");
                        DrawBosLevel(_relevantSwingLevelForBOS, _relevantSwingBarTimeNY);
                        return; // Found swing, exit
                    }
                }
                Print($"No relevant M3 Swing High found for BOS within {SwingLookbackPeriod} bars on {_executionTimeFrame} prior to M15 sweep event at {m15SweepBarOpenTimeNY:HH:mm} NY.");
            }
            else if (m15LastSweepType == SweepType.HighSwept) // After M15 High sweep, look for an M3 Swing Low to break (Bearish BOS)
            {
                for (int i = loopEndIndex; i >= loopStartIndex; i--)
                {
                    if (i < SwingCandles || i >= executionBars.ClosePrices.Count - SwingCandles) continue; 
                    
                    if (IsSwingLow(i, executionBars, SwingCandles))
                    {
                        _relevantSwingLevelForBOS = executionBars.LowPrices[i];
                        _relevantSwingBarTimeNY = GetNewYorkTime(executionBars.OpenTimes[i]);
                        Print($"Relevant M3 Swing Low for BOS identified at {Math.Round(_relevantSwingLevelForBOS, Symbol.Digits)} (Bar: {_relevantSwingBarTimeNY:yyyy-MM-dd HH:mm} NY on {_executionTimeFrame}) after M15 High Sweep.");
                        DrawBosLevel(_relevantSwingLevelForBOS, _relevantSwingBarTimeNY);
                        return; // Found swing, exit
                    }
                }
                Print($"No relevant M3 Swing Low found for BOS within {SwingLookbackPeriod} bars on {_executionTimeFrame} prior to M15 sweep event at {m15SweepBarOpenTimeNY:HH:mm} NY.");
            }

            // If we reach here, no swing was found and returned from within the loops.
            if (_relevantSwingLevelForBOS == 0)
            {
                ResetSweepAndBosState($"IdentifyRelevantM3SwingForBOS: No M3 swing found. Resetting M15 sweep state for {m15LastSweepType}.");
            }
        }

        private void CheckForBOSAndFVG(DateTime currentTimeNY)
        {
            if (_lastSweepType == SweepType.None || _relevantSwingLevelForBOS == 0 || _sweepBarOpenTimeNY == DateTime.MinValue)
            {
                return;
            }

            var executionBars = MarketData.GetBars(_executionTimeFrame);
            if (executionBars.Count < SwingCandles * 2 + 1) // Need enough bars for FVG and BOS checks
            {
                Print($"CheckForBOSAndFVG: Not enough bars on {_executionTimeFrame} ({executionBars.Count}) to proceed.");
                return;
            }

            DateTime sweepBarTimeUtc = TimeZoneInfo.ConvertTime(_sweepBarOpenTimeNY, _newYorkTimeZone, TimeZoneInfo.Utc);
            // Find the execution bar index that is at or after the M15 sweep bar's open time
            int firstBarToCheckForBOSIndexOnExecutionTF = executionBars.OpenTimes.GetIndexByTime(sweepBarTimeUtc);
            if (firstBarToCheckForBOSIndexOnExecutionTF < 0) {
                // If exact time not found, try next available bar
                for(int i = 0; i < executionBars.Count; i++)
                {
                    if (executionBars.OpenTimes[i] >= sweepBarTimeUtc)
                    {
                        firstBarToCheckForBOSIndexOnExecutionTF = i;
                        break;
                    }
                }
                if (firstBarToCheckForBOSIndexOnExecutionTF < 0)
                {
                    Print($"Error finding execution bar index in CheckForBOSAndFVG for M15 sweep bar time: {_sweepBarOpenTimeNY:yyyy-MM-dd HH:mm} (UTC: {sweepBarTimeUtc}). Resetting.");
                    _lastSweepType = SweepType.None;
                    _relevantSwingLevelForBOS = 0;
                    ClearBosAndFvgDrawings();
                    return;
                }
            }
            
            // Loop for BOS candidate bar on execution timeframe (e.g., M3)
            // Start from the second to last bar, as the last bar is still forming.
            for (int bosCandidateBarIndex = executionBars.Count - 2; bosCandidateBarIndex > firstBarToCheckForBOSIndexOnExecutionTF; bosCandidateBarIndex--) 
            {
                DateTime bosCandidateBarOpenTimeNY = GetNewYorkTime(executionBars.OpenTimes[bosCandidateBarIndex]);
                // Ensure bosCandidateBarIndex is not before or at the M15 swing bar time itself for BOS check.
                // _relevantSwingBarTimeNY is the open time of the M15 swing bar.
                if (_relevantSwingBarTimeNY != DateTime.MinValue && bosCandidateBarOpenTimeNY <= _relevantSwingBarTimeNY) 
                    continue;

                bool bosConfirmedThisBar = false;
                if (_lastSweepType == SweepType.HighSwept && executionBars.ClosePrices[bosCandidateBarIndex] < _relevantSwingLevelForBOS) 
                {
                    _bosLevel = _relevantSwingLevelForBOS; 
                    _bosTimeNY = bosCandidateBarOpenTimeNY; // M3 bar time
                    Print($"BEARISH BOS DETECTED on {_executionTimeFrame} at bar {_bosTimeNY:yyyy-MM-dd HH:mm} NY. Structure broken below Swing Low (M15): {Math.Round(_relevantSwingLevelForBOS, Symbol.Digits)}. Sweep was: {_lastSweepType}");
                    bosConfirmedThisBar = true;
                    DrawM3BosConfirmationLine(GetNewYorkTime(executionBars.OpenTimes[bosCandidateBarIndex]), SweepType.HighSwept);
                }
                else if (_lastSweepType == SweepType.LowSwept && executionBars.ClosePrices[bosCandidateBarIndex] > _relevantSwingLevelForBOS) 
                {
                    _bosLevel = _relevantSwingLevelForBOS;
                    _bosTimeNY = bosCandidateBarOpenTimeNY; // M3 bar time
                    Print($"BULLISH BOS DETECTED on {_executionTimeFrame} at bar {_bosTimeNY:yyyy-MM-dd HH:mm} NY. Structure broken above Swing High (M15): {Math.Round(_relevantSwingLevelForBOS, Symbol.Digits)}. Sweep was: {_lastSweepType}");
                    bosConfirmedThisBar = true;
                    DrawM3BosConfirmationLine(GetNewYorkTime(executionBars.OpenTimes[bosCandidateBarIndex]), SweepType.LowSwept);
                }

                if (bosConfirmedThisBar)
                {
                    // Now search for FVG on execution TF (e.g. M3)
                    // from bosCandidateBarIndex down to firstBarToCheckForBOSIndexOnExecutionTF (M3 bar corresponding to M15 sweep bar)
                    for (int fvgSearchIndex = bosCandidateBarIndex; fvgSearchIndex >= firstBarToCheckForBOSIndexOnExecutionTF; fvgSearchIndex--)
                    {
                        // Ensure we have enough bars for FVG (N, N+1, N+2 pattern means fvgSearchIndex (N+1 bar) must be at least index 1 for N-1)
                        // So, N-1 index is fvgSearchIndex - 1. N+1 index is fvgSearchIndex. N+2 index is fvgSearchIndex + 1
                        // This means index for N-1 (first bar of pattern) is fvgSearchIndex -1.
                        // Index for N+1 (middle bar) is fvgSearchIndex.
                        // Index for N+2 (last bar of pattern) is fvgSearchIndex + 1.
                        // The FVG is between bar (fvgSearchIndex-1) and bar (fvgSearchIndex+1)
                        // The 'series' passed to FindBullish/BearishFVG expects the index of the N+1 bar.
                        if (fvgSearchIndex < 1 || fvgSearchIndex + 1 >= executionBars.Count) continue;

                        double fvgLowBoundary, fvgHighBoundary;
                        bool fvgFound = false;

                        if (_lastSweepType == SweepType.HighSwept) // Bearish BOS, look for Bearish FVG
                        {
                            // Pass fvgSearchIndex (which is N+1 bar) to FindBearishFVG
                            if (FindBearishFVG(fvgSearchIndex, executionBars, out fvgLowBoundary, out fvgHighBoundary))
                            {
                                Print($"Bearish FVG found on {_executionTimeFrame} based on bar {GetNewYorkTime(executionBars.OpenTimes[fvgSearchIndex]):yyyy-MM-dd HH:mm} NY. Range: {fvgLowBoundary}-{fvgHighBoundary}");
                                _lastFvgDetermined_Low = fvgLowBoundary;
                                _lastFvgDetermined_High = fvgHighBoundary;
                                // Store details of the N (fvgSearchIndex-1) and N+2 (fvgSearchIndex+1) bars of the FVG pattern on Execution TF
                                _fvgBarN_OpenTimeNY = GetNewYorkTime(executionBars.OpenTimes[fvgSearchIndex - 1]);
                                _fvgBarN2_OpenTimeNY = GetNewYorkTime(executionBars.OpenTimes[fvgSearchIndex + 1]);
                                // Drawing FVG rectangle - using execution TF bar times.
                                // The GetTimeFrameTimeSpan used by DrawFvgRectangle should ideally be for _executionTimeFrame
                                DrawFvgRectangle(_fvgBarN_OpenTimeNY, _lastFvgDetermined_High, _fvgBarN2_OpenTimeNY, _lastFvgDetermined_Low, _executionTimeFrame);                                
                                PrepareAndPlaceFVGEntryOrder(SweepType.HighSwept); 
                                fvgFound = true;
                            }
                        }
                        else // Bullish BOS, look for Bullish FVG
                        {
                            if (FindBullishFVG(fvgSearchIndex, executionBars, out fvgLowBoundary, out fvgHighBoundary))
                            {
                                Print($"Bullish FVG found on {_executionTimeFrame} based on bar {GetNewYorkTime(executionBars.OpenTimes[fvgSearchIndex]):yyyy-MM-dd HH:mm} NY. Range: {fvgLowBoundary}-{fvgHighBoundary}");
                                _lastFvgDetermined_Low = fvgLowBoundary;
                                _lastFvgDetermined_High = fvgHighBoundary;
                                _fvgBarN_OpenTimeNY = GetNewYorkTime(executionBars.OpenTimes[fvgSearchIndex - 1]);
                                _fvgBarN2_OpenTimeNY = GetNewYorkTime(executionBars.OpenTimes[fvgSearchIndex + 1]);
                                DrawFvgRectangle(_fvgBarN_OpenTimeNY, _lastFvgDetermined_High, _fvgBarN2_OpenTimeNY, _lastFvgDetermined_Low, _executionTimeFrame);
                                PrepareAndPlaceFVGEntryOrder(SweepType.LowSwept); 
                                fvgFound = true;
                            }
                        }

                        if (fvgFound)
                        {
                            _bosLevel = 0; 
                            _relevantSwingLevelForBOS = 0;
                            return; 
                        }
                    }
                    Print($"BOS confirmed on {_executionTimeFrame} at {_bosTimeNY:yyyy-MM-dd HH:mm} NY, but NO FVG found in range from M15 sweep bar to BOS bar on {_executionTimeFrame}. Resetting.");
                    _lastSweepType = SweepType.None; 
                    _relevantSwingLevelForBOS = 0;
                    _sweepBarOpenTimeNY = DateTime.MinValue;
                    _sweepBarActualHighOnContextTF = 0;
                    _sweepBarActualLowOnContextTF = 0;
                    ClearBosAndFvgDrawings();
                    return;
                }
            }
            
            // Timeout for BOS detection on execution timeframe
            // If M15 sweep bar was X minutes ago, and execution TF is M3, then X/3 bars.
            // Original was: 15 M15 bars = 15 * 15 = 225 minutes.
            // For M3, this would be 225 / 3 = 75 bars.
            // For M1, this would be 225 bars.
            int executionTimeFrameMinutes = GetTimeFrameInMinutes(_executionTimeFrame);
            if (executionTimeFrameMinutes == 0) executionTimeFrameMinutes = 3; // safety for division
            int bosTimeoutBars = (15 * GetTimeFrameInMinutes(_contextTimeFrame)) / executionTimeFrameMinutes;


            if (executionBars.Count -1 > firstBarToCheckForBOSIndexOnExecutionTF + bosTimeoutBars && firstBarToCheckForBOSIndexOnExecutionTF >=0) 
            {
                ResetSweepAndBosState($"No BOS detected on {_executionTimeFrame} within approx. {15 * GetTimeFrameInMinutes(_contextTimeFrame)} mins of M15 sweep. Resetting sweep state for {_lastSweepType}. M15 Sweep bar was {_sweepBarOpenTimeNY:HH:mm}");
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
            // Check daily trading limits before proceeding
            if (_tradesTakenToday >= 3)
            {
                Print("Daily trade limit (3) reached. No more trades today.");
                _lastSweepType = SweepType.None;
                _relevantSwingLevelForBOS = 0;
                _sweepBarOpenTimeNY = DateTime.MinValue;
                _sweepBarActualHighOnContextTF = 0;
                _sweepBarActualLowOnContextTF = 0;
                ClearBosAndFvgDrawings();
                return;
            }

            if (_dailyProfitTargetMet)
            {
                Print("Daily profit target (>=1%) met. No more trades today.");
                _lastSweepType = SweepType.None;
                _relevantSwingLevelForBOS = 0;
                _sweepBarOpenTimeNY = DateTime.MinValue;
                _sweepBarActualHighOnContextTF = 0;
                _sweepBarActualLowOnContextTF = 0;
                ClearBosAndFvgDrawings();
                return;
            }

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
                ResetSweepAndBosState($"Buy Limit entry {entryPrice} is at or above current Ask {Symbol.Ask}. Order not placed. Full state reset.");
                return; 
            }
            if (tradeType == TradeType.Sell && entryPrice <= Symbol.Bid)
            {
                ResetSweepAndBosState($"Sell Limit entry {entryPrice} is at or below current Bid {Symbol.Bid}. Order not placed. Full state reset.");
                return;
            }

            Print($"Preparing to place {tradeType} Limit Order: Label={newOrderLabel}, Vol={volume}, Entry={Math.Round(entryPrice, Symbol.Digits)}, SL={Math.Round(stopLossPrice, Symbol.Digits)} ({stopLossInPips} pips), TP={Math.Round(takeProfitPrice, Symbol.Digits)}");

            // Explicitly cast the 8th argument (null) to ProtectionType? to resolve ambiguity
            PlaceLimitOrderAsync(tradeType, SymbolName, volume, entryPrice, newOrderLabel, stopLossPrice, takeProfitPrice, (ProtectionType?)null, tradeResult =>
            {
                if (tradeResult.IsSuccessful)
                {
                    Print($"Successfully placed pending order {newOrderLabel}. Position ID (if filled immediately): {tradeResult.Position?.Id}, Pending Order ID: {tradeResult.PendingOrder?.Id}");
                    DrawPendingOrderLines(entryPrice, stopLossPrice, takeProfitPrice);
                    _currentPendingOrderLabel = newOrderLabel;
                }
                else
                {
                    Print($"Failed to place pending order {newOrderLabel}: {tradeResult.Error}. Clearing related drawings.");
                    ClearBosAndFvgDrawings(); // Also clear FVG if order placement failed
                    _lastSweepType = SweepType.None;
                    _relevantSwingLevelForBOS = 0;
                    _sweepBarOpenTimeNY = DateTime.MinValue;
                    _sweepBarActualHighOnContextTF = 0;
                    _sweepBarActualLowOnContextTF = 0;
                }
            });
        }

        private void OnPendingOrderFilled(PendingOrderFilledEventArgs args)
        {
            Print($"Pending order {args.PendingOrder.Label} filled and became position {args.Position.Id} (Symbol: {args.Position.SymbolName}, Type: {args.Position.TradeType}, Volume: {args.Position.VolumeInUnits}, Entry: {args.Position.EntryPrice}).");
            if (args.PendingOrder.Label == _currentPendingOrderLabel)
            {
                _currentPendingOrderLabel = null; 
                ClearPendingOrderLines();
                // FVG and BOS drawings remain until daily reset or next setup
            }
        }

        private void OnPendingOrderCancelled(PendingOrderCancelledEventArgs args)
        {
            Print($"Pending order {args.PendingOrder.Label} was cancelled. Reason: {args.Reason}");
            if (args.PendingOrder.Label == _currentPendingOrderLabel)
            {
                _currentPendingOrderLabel = null;
                ClearPendingOrderLines();
                ClearBosAndFvgDrawings(); // If order cancelled, assume setup is invalid
                _lastSweepType = SweepType.None; // Reset state to allow new sweep detection
                _relevantSwingLevelForBOS = 0;
                _sweepBarOpenTimeNY = DateTime.MinValue;
            }
        }

        private void OnPositionOpened(PositionOpenedEventArgs args)
        { 
            Print($"Position {args.Position.Id} opened (Symbol: {args.Position.SymbolName}, Type: {args.Position.TradeType}, Volume: {args.Position.VolumeInUnits}, Entry: {args.Position.EntryPrice}, SL: {args.Position.StopLoss}, TP: {args.Position.TakeProfit}).");
            _tradesTakenToday++;
            Print($"Trade count for today: {_tradesTakenToday}.");
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

            // Check for daily profit target only if it hasn't been met yet and the trade was profitable
            if (!_dailyProfitTargetMet && position.GrossProfit > 0)
            {
                double profitOfThisTradePercentage = (position.GrossProfit / Account.Balance) * 100;
                Print($"Profit from closed position {position.Id}: {position.GrossProfit} ({profitOfThisTradePercentage:F2}% of current balance).");

                // Using 1.0 as the threshold for >=1% profit
                if (profitOfThisTradePercentage >= 1.0) 
                {
                    _dailyProfitTargetMet = true;
                    Print($"Daily profit target of >=1% met with this trade ({profitOfThisTradePercentage:F2}%). No more trades will be opened today.");

                    // If a pending order exists, cancel it as the daily profit target is met
                    if (!string.IsNullOrEmpty(_currentPendingOrderLabel))
                    {
                        var pendingOrderToCancel = PendingOrders.FirstOrDefault(o => o.Label == _currentPendingOrderLabel);
                        if (pendingOrderToCancel != null)
                        {
                            CancelPendingOrderAsync(pendingOrderToCancel, (cancelResult) => 
                            {
                                if(cancelResult.IsSuccessful) Print($"Pending order {pendingOrderToCancel.Label} cancelled as daily profit target met.");
                                else Print($"Failed to cancel pending order {pendingOrderToCancel.Label} after profit target met: {cancelResult.Error}");
                            });
                            _currentPendingOrderLabel = null; 
                            ClearPendingOrderLines(); // Clear its drawings
                        }
                    }
                }
            }
        }

        // Helper method to get TimeFrame duration in minutes
        private int GetTimeFrameInMinutes(TimeFrame timeFrame)
        {
            if (timeFrame == TimeFrame.Minute) return 1;
            if (timeFrame == TimeFrame.Minute2) return 2;
            if (timeFrame == TimeFrame.Minute3) return 3;
            if (timeFrame == TimeFrame.Minute4) return 4;
            if (timeFrame == TimeFrame.Minute5) return 5;
            if (timeFrame == TimeFrame.Minute10) return 10;
            if (timeFrame == TimeFrame.Minute15) return 15;
            if (timeFrame == TimeFrame.Minute30) return 30;
            if (timeFrame == TimeFrame.Hour) return 60;
            if (timeFrame == TimeFrame.Hour4) return 240;
            if (timeFrame == TimeFrame.Daily) return 1440;
            
            Print($"Warning: GetTimeFrameInMinutes does not have a specific case for {timeFrame}. Defaulting to 1 minute as a fallback.");
            return 1; // Defaulting to 1 minute as a safer fallback than 15.
        }

        // --- Chart Drawing Methods ---
        private TimeSpan GetTimeFrameTimeSpan(TimeFrame timeFrame)
        {
            if (timeFrame == TimeFrame.Minute) return TimeSpan.FromMinutes(1);
            if (timeFrame == TimeFrame.Minute2) return TimeSpan.FromMinutes(2);
            if (timeFrame == TimeFrame.Minute3) return TimeSpan.FromMinutes(3);
            if (timeFrame == TimeFrame.Minute4) return TimeSpan.FromMinutes(4);
            if (timeFrame == TimeFrame.Minute5) return TimeSpan.FromMinutes(5);
            if (timeFrame == TimeFrame.Minute10) return TimeSpan.FromMinutes(10);
            if (timeFrame == TimeFrame.Minute15) return TimeSpan.FromMinutes(15);
            if (timeFrame == TimeFrame.Minute30) return TimeSpan.FromMinutes(30);
            if (timeFrame == TimeFrame.Hour) return TimeSpan.FromHours(1);
            if (timeFrame == TimeFrame.Hour4) return TimeSpan.FromHours(4);
            if (timeFrame == TimeFrame.Daily) return TimeSpan.FromDays(1);
            Print($"Warning: GetTimeFrameTimeSpan does not have a specific case for {timeFrame}. Defaulting to 1 minute as a fallback.");
            return TimeSpan.FromMinutes(1); // Defaulting to 1 minute as a safer fallback.
        }

        private void DrawLiquidityLevels(double high, double low)
        {
            Chart.RemoveObject(LiqHighLineName);
            Chart.RemoveObject(LiqLowLineName);
            if (high != 0) Chart.DrawHorizontalLine(LiqHighLineName, high, Color.Blue, 2, LineStyle.Solid);
            if (low != 0) Chart.DrawHorizontalLine(LiqLowLineName, low, Color.Red, 2, LineStyle.Solid);
        }

        private void DrawBosLevel(double level, DateTime swingBarOpenTimeNY)
        {
            Chart.RemoveObject(BosSwingLineName);
            if (level != 0 && swingBarOpenTimeNY != DateTime.MinValue)
            {
                DateTime startTimeServer = TimeZoneInfo.ConvertTime(swingBarOpenTimeNY, _newYorkTimeZone, TimeZone);
                // Extend the line for a fixed number of execution bars (e.g., 10 bars)
                TimeSpan executionBarDuration = GetTimeFrameTimeSpan(_executionTimeFrame);
                DateTime endTimeServer = startTimeServer.Add(TimeSpan.FromTicks(executionBarDuration.Ticks * 10)); 

                Chart.DrawTrendLine(BosSwingLineName, startTimeServer, level, endTimeServer, level, Color.Orange, 2, LineStyle.Dots);
            }
        }

        private void DrawFvgRectangle(DateTime startTimeNY, double topPrice, DateTime endTimeNY, double bottomPrice, TimeFrame frameForDuration)
        {
            Chart.RemoveObject(FvgRectName);
            TimeSpan barDuration = GetTimeFrameTimeSpan(frameForDuration);

            DateTime actualEndTimeNY = endTimeNY;
            if (endTimeNY <= startTimeNY)
            {
                actualEndTimeNY = startTimeNY.Add(barDuration); 
            }

            DateTime startTimeServer = TimeZoneInfo.ConvertTime(startTimeNY, _newYorkTimeZone, TimeZone);
            DateTime endTimeServer = TimeZoneInfo.ConvertTime(actualEndTimeNY, _newYorkTimeZone, TimeZone);

            double rectTop = Math.Max(topPrice, bottomPrice);
            double rectBottom = Math.Min(topPrice, bottomPrice);

            if (topPrice != 0 && bottomPrice != 0)
            {
                // For FVG, the rectangle width should represent the duration of the FVG pattern (typically 3 bars of the given timeframe)
                // If startTimeNY is N-bar open and endTimeNY is N+2 bar open, we need N+2 bar close for full rect.
                // Let's make it span from N-1 open to N+2 open + N+2 duration
                var rect = Chart.DrawRectangle(FvgRectName, startTimeServer, rectTop, endTimeServer.Add(barDuration), rectBottom, Color.FromArgb(80, Color.Gray.R, Color.Gray.G, Color.Gray.B));
                rect.IsFilled = true;
            }
        }

        private void DrawPendingOrderLines(double entry, double sl, double tp)
        {
            ClearPendingOrderLines();
            if (entry != 0) Chart.DrawHorizontalLine(PendingEntryLineName, entry, Color.Green, 2, LineStyle.Dots);
            if (sl != 0) Chart.DrawHorizontalLine(PendingSlLineName, sl, Color.Red, 2, LineStyle.Dots);
            if (tp != 0) Chart.DrawHorizontalLine(PendingTpLineName, tp, Color.DodgerBlue, 2, LineStyle.Dots);
        }

        private void ClearLiquidityDrawings()
        {
            Chart.RemoveObject(LiqHighLineName);
            Chart.RemoveObject(LiqLowLineName);
        }

        private void ClearBosAndFvgDrawings()
        {
            Chart.RemoveObject(BosSwingLineName);
            Chart.RemoveObject(FvgRectName);
            Chart.RemoveObject(M3BosConfirmationLineName);
        }

        private void ClearPendingOrderLines()
        {
            Chart.RemoveObject(PendingEntryLineName);
            Chart.RemoveObject(PendingSlLineName);
            Chart.RemoveObject(PendingTpLineName);
        }

        private void ClearAllStrategyDrawings()
        {
            ClearLiquidityDrawings();
            ClearBosAndFvgDrawings();
            ClearPendingOrderLines();
        }

        private void DrawM3BosConfirmationLine(DateTime bosConfirmationBarTimeNY, SweepType originalSweepDirection)
        {
            Chart.RemoveObject(M3BosConfirmationLineName); // Remove previous one, if any
            DateTime serverTime = TimeZoneInfo.ConvertTime(bosConfirmationBarTimeNY, _newYorkTimeZone, TimeZone);
            Color lineColor;
            // The color depends on the direction of the BOS, which is opposite to the initial sweep for the swing identification,
            // but same direction logic as _lastSweepType leading to the BOS check.
            if (originalSweepDirection == SweepType.LowSwept) // Original M15 sweep was low, so M3 BOS is UP (Bullish)
            {
                lineColor = Color.DarkGreen;
            }
            else // Original M15 sweep was high, so M3 BOS is DOWN (Bearish)
            {
                lineColor = Color.DarkRed;
            }
            Chart.DrawVerticalLine(M3BosConfirmationLineName, serverTime, lineColor, 2, LineStyle.Solid);
        }

        private void UpdateTrendContext(DateTime currentTimeNY)
        {
            var contextTFbars = MarketData.GetBars(_contextTimeFrameForTrend);
            if (contextTFbars.Count < ContextLookbackCandles + 1) // Need at least LookbackCandles + 1 current bar
            {
                // Not enough data, or not enough historical data to form a lookback from a fully closed bar
                if (_currentTrendContext != TrendContext.None)
                {
                     Print($"Trend Context: Not enough data on {_contextTimeFrameForTrend} to determine trend (requires {ContextLookbackCandles + 1} bars, have {contextTFbars.Count}). Setting to None.");
                    _currentTrendContext = TrendContext.None;
                    _lastContextTrendCalculationTimeNY = DateTime.MinValue; // Reset to allow recalculation when data is available
                }
                return;
            }

            // Use the last fully closed bar for calculation
            int lastClosedBarIndex = contextTFbars.Count - 2;
            DateTime lastClosedBarOpenTimeNY = GetNewYorkTime(contextTFbars.OpenTimes[lastClosedBarIndex]);

            if (lastClosedBarOpenTimeNY == _lastContextTrendCalculationTimeNY)
            {
                return; // Already calculated for this bar
            }

            int bullishCount = 0;
            int bearishCount = 0;
            int firstBarToAnalyze = Math.Max(0, lastClosedBarIndex - ContextLookbackCandles + 1);

            for (int i = firstBarToAnalyze; i <= lastClosedBarIndex; i++)
            {
                if (contextTFbars.ClosePrices[i] > contextTFbars.OpenPrices[i])
                {
                    bullishCount++;
                }
                else if (contextTFbars.ClosePrices[i] < contextTFbars.OpenPrices[i])
                {
                    bearishCount++;
                }
            }

            TrendContext previousTrendContext = _currentTrendContext;
            if (bullishCount - bearishCount >= ContextTrendThreshold)
            {
                _currentTrendContext = TrendContext.Bullish;
            }
            else if (bearishCount - bullishCount >= ContextTrendThreshold)
            {
                _currentTrendContext = TrendContext.Bearish;
            }
            else
            {
                _currentTrendContext = TrendContext.None;
            }

            if (_currentTrendContext != previousTrendContext || _lastContextTrendCalculationTimeNY == DateTime.MinValue) // Log on change or first run
            {
                Print($"Trend Context ({_contextTimeFrameForTrend}@{ContextLookbackCandles} candles, Thr:{ContextTrendThreshold}): {_currentTrendContext}. Bull: {bullishCount}, Bear: {bearishCount}. Based on bar: {lastClosedBarOpenTimeNY:yyyy-MM-dd HH:mm} NY.");
            }
            _lastContextTrendCalculationTimeNY = lastClosedBarOpenTimeNY;
        }

        private void ResetSweepAndBosState(string reason)
        {
            if (!string.IsNullOrWhiteSpace(reason)) // Only print if reason is provided
            {
                Print(reason);
            }

            _lastSweepType = SweepType.None;
            _relevantSwingLevelForBOS = 0;
            _relevantSwingBarTimeNY = DateTime.MinValue;
            _sweepBarOpenTimeNY = DateTime.MinValue;
            _sweepBarActualHighOnContextTF = 0;
            _sweepBarActualLowOnContextTF = 0;
            
            _bosLevel = 0;
            _bosTimeNY = DateTime.MinValue;
            _fvgFoundOnBarOpenTimeNY = DateTime.MinValue;
            _lastFvgDetermined_High = 0;
            _lastFvgDetermined_Low = 0;
            _fvgBarN_OpenTimeNY = DateTime.MinValue;
            _fvgBarN2_OpenTimeNY = DateTime.MinValue;

            // Clear drawings
            ClearBosAndFvgDrawings(); // Clears BosSwingLineName and FvgRectName
            Chart.RemoveObject(M3BosConfirmationLineName);
            // Note: Liquidity lines (LiqHighLineName, LiqLowLineName) are managed by DailyReset or when new liquidity is identified.
            // M3 Sweep visualization lines are managed by VisualizeM3Sweeps itself (cleared on each run).
        }

        private TimeFrame ParseTimeFrame(string timeFrameString)
        {
            if (string.IsNullOrWhiteSpace(timeFrameString))
            {
                throw new ArgumentException("TimeFrame string cannot be empty.");
            }

            switch (timeFrameString.ToLowerInvariant())
            {
                case "m1": case "minute1": return TimeFrame.Minute;
                case "m2": case "minute2": return TimeFrame.Minute2;
                case "m3": case "minute3": return TimeFrame.Minute3;
                case "m4": case "minute4": return TimeFrame.Minute4;
                case "m5": case "minute5": return TimeFrame.Minute5;
                case "m10": case "minute10": return TimeFrame.Minute10;
                case "m15": case "minute15": return TimeFrame.Minute15;
                case "m30": case "minute30": return TimeFrame.Minute30;
                case "h1": case "hour1": return TimeFrame.Hour;
                case "h4": case "hour4": return TimeFrame.Hour4;
                case "d1": case "daily": return TimeFrame.Daily;
                case "w1": case "weekly": return TimeFrame.Weekly;
                case "mn1": case "monthly": return TimeFrame.Monthly;
                default:
                    throw new ArgumentException($"Could not parse TimeFrame string: {timeFrameString}");
            }
        }
    }
}
