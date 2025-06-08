using System;
using System.Linq;
using System.Collections.Generic;
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

    public enum EntryStrategyType
    {
        OnFVG, // Rename and change to BOS_FVG_TEST
        OnBOS // BOS (closed candle) -> enter market order
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
        private const string M1BosConfirmationLineName = "SB_M1_BOS_Confirmation_Line";
        private const string SweepLineName = "SB_SweepLine";

        // Text Object Names
        private const string LiqHighTextName = "SB_LiqHighText";
        private const string LiqLowTextName = "SB_LiqLowText";
        private const string BosLevelTextName = "SB_BosLevelText";
        private const string FvgTextName = "SB_FvgText";
        private const string PendingEntryTextName = "SB_PendingEntryText";
        private const string PendingSlTextName = "SB_PendingSlText";
        private const string PendingTpTextName = "SB_PendingTpText";
        private const string SweepTextName = "SB_SweepText";
        private const string M1BosConfirmationTextName = "SB_M1BosConfirmationText";

        // Strategy Constants
        private const double RiskPercentage = 2.0;
        private const string ContextTimeFrameString = "m1";
        private const string ExecutionTimeFrameString = "m1";
        private const int MaxLiquidityLevelsToDraw = 10;

        // Daily Trading Limits
        private int _tradesTakenToday = 0;
        private bool _dailyProfitTargetMet = false;

        [Parameter("Minutes Before Session for Liquidity", DefaultValue = 10, MinValue = 1, Group = "Session Timing")]
        public int MinutesBeforeSessionForLiquidity { get; set; }

        [Parameter("Swing Lookback Period", DefaultValue = 180, MinValue = 30, Group = "Strategy Parameters")]
        public int SwingLookbackPeriod { get; set; }

        [Parameter("Swing Candles", DefaultValue = 1, MinValue = 1, MaxValue = 3, Group = "Strategy Parameters")]
        public int SwingCandles { get; set; }

        [Parameter("Min RR", DefaultValue = 1.3, Group = "Strategy Parameters")]
        public double MinRiskRewardRatio { get; set; }

        [Parameter("Max RR", DefaultValue = 4.0, Group = "Strategy Parameters")]
        public double MaxRiskRewardRatio { get; set; }

        [Parameter("Entry Strategy", DefaultValue = EntryStrategyType.OnFVG, Group = "Strategy Parameters")]
        public EntryStrategyType EntryStrategy { get; set; }

        // New York TimeZoneInfo
        private TimeZoneInfo _newYorkTimeZone;
        private TimeFrame _contextTimeFrame;
        private TimeFrame _executionTimeFrame;

        // Session timing and liquidity state
        private DateTime _session1ActualStartNY, _session2ActualStartNY, _session3ActualStartNY;
        private DateTime _session1ActualEndNY, _session2ActualEndNY, _session3ActualEndNY;
        private bool _liquidityIdentifiedForSession1, _liquidityIdentifiedForSession2, _liquidityIdentifiedForSession3;
        
        private List<Tuple<double, DateTime>> _liquidityHighs;
        private List<Tuple<double, DateTime>> _liquidityLows;
        private DateTime _lastDateProcessedForSessionTimes = DateTime.MinValue;
        private List<Tuple<double, DateTime>> _tpTargetLiquidityLevels; // To store liquidity levels for TP targeting

        private SweepType _lastSweepType = SweepType.None;
        private double _lastSweptLiquidityLevel;
        private DateTime _timeOfLastSweepNY = DateTime.MinValue; // Precise time of sweep (tick time)
        
        // Sweep Bar details on Execution TimeFrame (M1)
        private DateTime _sweepBarTimeNY = DateTime.MinValue;

        private double _relevantSwingLevelForBOS = 0; // The specific high/low of the M1 swing to be broken for BOS
        private DateTime _relevantSwingBarTimeNY = DateTime.MinValue; // NYTime of the M1 bar that formed the _relevantSwingLevelForBOS
        private double _bosLevel = 0; // Level at which BOS was confirmed
        private DateTime _bosTimeNY = DateTime.MinValue; // Time BOS was confirmed (NY) on execution TF
        private DateTime _fvgFoundOnBarOpenTimeNY = DateTime.MinValue; // NY Time of the M1 bar on which FVG was found
        private double _lastFvgDetermined_High = 0;
        private double _lastFvgDetermined_Low = 0;
        private DateTime _fvgBarN_OpenTimeNY = DateTime.MinValue; // FVG Bar N (first bar of 3-bar FVG pattern) OpenTime NY
        private DateTime _fvgBarN2_OpenTimeNY = DateTime.MinValue; // FVG Bar N+2 (third bar of 3-bar FVG pattern) OpenTime NY
        private string _currentPendingOrderLabel = null;
        private DateTime _lastDailyResetTimeNY = DateTime.MinValue;

        // Dictionary to hold SL/TP info against a unique order label
        private Dictionary<string, Tuple<double, double>> _pendingOrderInfo = new Dictionary<string, Tuple<double, double>>();

        private TimeSpan _contextTimeFrameTimeSpan; // To help with FVG rect drawing duration

        private bool _isProcessingSetup = false; // Flag to prevent re-triggering on the same setup

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

            _liquidityHighs = new List<Tuple<double, DateTime>>();
            _liquidityLows = new List<Tuple<double, DateTime>>();
            _tpTargetLiquidityLevels = new List<Tuple<double, DateTime>>();

            try
            {
                _contextTimeFrame = TimeFrame.Parse(ContextTimeFrameString);
            }
            catch (ArgumentException)
            {
                Print($"Error parsing ContextTimeFrameString: '{ContextTimeFrameString}'. Defaulting to m1.");
                _contextTimeFrame = TimeFrame.Minute;
            }
            _contextTimeFrameTimeSpan = GetTimeFrameTimeSpan(_contextTimeFrame); // Initialize TimeSpan for context timeframe

            try
            {
                _executionTimeFrame = TimeFrame.Parse(ExecutionTimeFrameString);
            }
            catch (ArgumentException)
            {
                Print($"Error parsing ExecutionTimeFrameString: '{ExecutionTimeFrameString}'. Defaulting to m1.");
                _executionTimeFrame = TimeFrame.Minute;
            }

            Print("Silver Bullet Bot Started.");
            Print("Trading Sessions (NY Time): 03:00-04:00, 10:00-11:00, 14:00-15:00");
            Print($"Liquidity lookback: {MinutesBeforeSessionForLiquidity} minutes before session start.");
            Print($"Risk per trade: {RiskPercentage}%, Min RR: {MinRiskRewardRatio}, Max RR: {MaxRiskRewardRatio}");
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

            // Do not process anything new if we are already handling a setup
            if (_isProcessingSetup) return;

            SessionInfo currentSession = GetCurrentSessionInfo(currentTimeNY);

            if (_lastSweepType != SweepType.None)
            {
                if (_relevantSwingLevelForBOS == 0) 
                {
                    IdentifyRelevantM1SwingForBOS(_sweepBarTimeNY, _lastSweepType);
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

                if (relevantLiquidityFlag && (_liquidityHighs.Count > 0 || _liquidityLows.Count > 0))
                {
                    CheckForLiquiditySweep(currentTimeNY);
                }
            }
            else
            {
                if (_liquidityHighs.Count > 0 || _liquidityLows.Count > 0)
                {
                    if (!_liquidityIdentifiedForSession1 && !_liquidityIdentifiedForSession2 && !_liquidityIdentifiedForSession3 && _lastSweepType == SweepType.None)
                    {
                        _liquidityHighs.Clear();
                        _liquidityLows.Clear();
                        ClearLiquidityDrawings();
                    }
                }
                if (_lastSweepType != SweepType.None && _relevantSwingLevelForBOS == 0) // If sweep happened but no swing found, reset to re-evaluate liquidity
                {
                     // This condition is handled inside IdentifyRelevantM1SwingForBOS now
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

            // ClearAllStrategyDrawings(); // Keep drawings on chart for analysis after backtest
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

                DrawSessionTimeLines();

                _liquidityIdentifiedForSession1 = false;
                _liquidityIdentifiedForSession2 = false;
                _liquidityIdentifiedForSession3 = false;
                _liquidityHighs.Clear();
                _liquidityLows.Clear();
                _lastSweepType = SweepType.None; // Reset sweep type for new day
                _relevantSwingLevelForBOS = 0;
                _sweepBarTimeNY = DateTime.MinValue;

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
            if (contextBars.Count < SwingLookbackPeriod)
            {
                Print("Error: Not enough context bars to identify liquidity.");
                return;
            }

            DateTime timeForLiquidityBarTargetNY = sessionActualStartNY.AddTicks(-1);
            DateTime timeForLiquidityBarTargetUtc;
            try
            {
                timeForLiquidityBarTargetUtc = TimeZoneInfo.ConvertTime(timeForLiquidityBarTargetNY, _newYorkTimeZone, TimeZoneInfo.Utc);
            }
            catch (Exception ex)
            {
                Print($"Error converting liquidity target time to UTC: {ex.Message}");
                return;
            }

            int endIndex = contextBars.OpenTimes.GetIndexByTime(timeForLiquidityBarTargetUtc);
            if (endIndex < 0)
            {
                Print($"Error: Could not find bar index for pre-session liquidity search. Target Time (NY): {timeForLiquidityBarTargetNY:T}");
                return;
            }

            int startIndex = Math.Max(0, endIndex - SwingLookbackPeriod);
            
            _liquidityHighs.Clear();
            _liquidityLows.Clear();

            double highestHighSince = 0;
            double lowestLowSince = double.MaxValue;

            // Find all *uncovered* swing points in the lookback period, iterating from recent to past
            for (int i = endIndex; i >= startIndex; i--)
            {
                // Check for an uncovered swing high
                if (IsSwingHigh(i, contextBars, SwingCandles))
                {
                    if (contextBars.HighPrices[i] > highestHighSince)
                    {
                        var barTimeNY = GetNewYorkTime(contextBars.OpenTimes[i]);
                        if ((sessionActualStartNY - barTimeNY).TotalMinutes >= MinutesBeforeSessionForLiquidity)
                        {
                            _liquidityHighs.Add(new Tuple<double, DateTime>(contextBars.HighPrices[i], barTimeNY));
                        }
                    }
                }

                // Check for an uncovered swing low
                if (IsSwingLow(i, contextBars, SwingCandles))
                {
                    if (contextBars.LowPrices[i] < lowestLowSince)
                    {
                        var barTimeNY = GetNewYorkTime(contextBars.OpenTimes[i]);
                        if ((sessionActualStartNY - barTimeNY).TotalMinutes >= MinutesBeforeSessionForLiquidity)
                        {
                            _liquidityLows.Add(new Tuple<double, DateTime>(contextBars.LowPrices[i], barTimeNY));
                        }
                    }
                }

                // Update the running min/max for the next iteration (which is further in the past)
                highestHighSince = Math.Max(highestHighSince, contextBars.HighPrices[i]);
                lowestLowSince = Math.Min(lowestLowSince, contextBars.LowPrices[i]);
            }
            
            // By not sorting by price, we prioritize the most recent swings found in the loop.
            // _liquidityHighs = _liquidityHighs.OrderByDescending(x => x.Item1).ToList();
            // _liquidityLows = _liquidityLows.OrderBy(x => x.Item1).ToList();

            if (_liquidityHighs.Count > 0 || _liquidityLows.Count > 0)
            {
                 SetLiquidityIdentifiedFlag(sessionNumber, true);
                Print($"Session {sessionNumber} ({sessionActualStartNY:HH:mm} NY): Liquidity identified. Found {_liquidityHighs.Count} Highs and {_liquidityLows.Count} Lows.");
                DrawLiquidityLevels(_liquidityHighs, _liquidityLows, sessionActualStartNY);
            }
            else
            {
                Print($"Warning for Session {sessionNumber}: No swing high/low found within {SwingLookbackPeriod} bars to define liquidity.");
            }
        }

        private void CheckForLiquiditySweep(DateTime currentTimeNY)
        {
            if ((_liquidityHighs.Count == 0 && _liquidityLows.Count == 0) || _lastSweepType != SweepType.None) return;

            var executionBars = MarketData.GetBars(_executionTimeFrame);
            if (executionBars.Count == 0) return;
            var lastBar = executionBars.Last();

            double currentAsk = Symbol.Ask;
            double currentBid = Symbol.Bid;
            SweepType detectedSweepThisTick = SweepType.None;
            Tuple<double, DateTime> sweptLevel = null;

            // Check for high sweeps
            foreach (var liqHigh in _liquidityHighs)
            {
                if (lastBar.High > liqHigh.Item1 || currentAsk > liqHigh.Item1)
                {
                    Print($"HIGH LIQUIDITY SWEPT at {liqHigh.Item1}. Price: {Math.Max(lastBar.High, currentAsk)}, Time: {currentTimeNY:HH:mm:ss} NY");
                    detectedSweepThisTick = SweepType.HighSwept;
                    _lastSweptLiquidityLevel = liqHigh.Item1;
                    sweptLevel = liqHigh;
                    break;
                }
            }

            // If no high was swept, check for low sweeps
            if (detectedSweepThisTick == SweepType.None)
            {
                foreach (var liqLow in _liquidityLows)
                {
                    if (lastBar.Low < liqLow.Item1 || currentBid < liqLow.Item1)
                    {
                        Print($"LOW LIQUIDITY SWEPT at {liqLow.Item1}. Price: {Math.Min(lastBar.Low, currentBid)}, Time: {currentTimeNY:HH:mm:ss} NY");
                        detectedSweepThisTick = SweepType.LowSwept;
                        _lastSweptLiquidityLevel = liqLow.Item1;
                        sweptLevel = liqLow;
                        break;
                    }
                }
            }


            if (detectedSweepThisTick != SweepType.None)
            {
                _lastSweepType = detectedSweepThisTick;
                _timeOfLastSweepNY = currentTimeNY; // Tick time of sweep

                if (detectedSweepThisTick == SweepType.HighSwept)
                {
                    _tpTargetLiquidityLevels = new List<Tuple<double, DateTime>>(_liquidityLows);
                    Print($"Copied {_tpTargetLiquidityLevels.Count} liquidity lows for potential TP targets.");
                }
                else // LowSwept
                {
                    _tpTargetLiquidityLevels = new List<Tuple<double, DateTime>>(_liquidityHighs);
                    Print($"Copied {_tpTargetLiquidityLevels.Count} liquidity highs for potential TP targets.");
                }
                
                int sweepBarIndex = executionBars.Count - 1;
                _sweepBarTimeNY = GetNewYorkTime(executionBars.OpenTimes[sweepBarIndex]);
                double sweepBarHigh = executionBars.HighPrices[sweepBarIndex];
                double sweepBarLow = executionBars.LowPrices[sweepBarIndex];

                if (detectedSweepThisTick == SweepType.HighSwept)
                {
                    DrawSweepLine(sweptLevel.Item2, _lastSweptLiquidityLevel, _sweepBarTimeNY, sweepBarHigh);
                    }
                else // LowSwept
                {
                    DrawSweepLine(sweptLevel.Item2, _lastSweptLiquidityLevel, _sweepBarTimeNY, sweepBarLow);
                }

                _liquidityHighs.Clear();
                _liquidityLows.Clear();
                
                Print($"Sweep occurred on M1 bar: {_sweepBarTimeNY:yyyy-MM-dd HH:mm} NY, H: {sweepBarHigh}, L: {sweepBarLow}");
                IdentifyRelevantM1SwingForBOS(_sweepBarTimeNY, _lastSweepType); 
            }
        }

        private bool IsSwingHigh(int barIndex, Bars series, int swingCandles = 1)
        {
            if (barIndex < swingCandles || barIndex >= series.Count - swingCandles)
                return false;

            double peakHigh = series.HighPrices[barIndex];
            double tolerance = Symbol.PipSize * 0.1; // 1/10th of a pip tolerance

            // Find the start of the plateau (j)
            int plateauStartIndex = barIndex;
            while (plateauStartIndex > 0 && Math.Abs(series.HighPrices[plateauStartIndex - 1] - peakHigh) < tolerance)
            {
                plateauStartIndex--;
            }

            // Now check if the bars to the left of the plateau are all lower
            for (int i = 1; i <= swingCandles; i++)
            {
                int leftIndex = plateauStartIndex - i;
                if (leftIndex < 0 || series.HighPrices[leftIndex] >= peakHigh - tolerance) // Must be clearly lower
                {
                    return false;
                }
            }

            // And check if the bars to the right of the original barIndex are all lower
            for (int i = 1; i <= swingCandles; i++)
            {
                if (barIndex + i >= series.Count || series.HighPrices[barIndex + i] > peakHigh + tolerance) // Must be clearly lower, relaxed condition
                {
                    return false;
                }
            }

            return true;
        }

        private bool IsSwingLow(int barIndex, Bars series, int swingCandles = 1)
        {
            if (barIndex < swingCandles || barIndex >= series.Count - swingCandles)
                return false;

            double peakLow = series.LowPrices[barIndex];
            double tolerance = Symbol.PipSize * 0.1; // 1/10th of a pip tolerance

            // Find the start of the plateau (j)
            int plateauStartIndex = barIndex;
            while (plateauStartIndex > 0 && Math.Abs(series.LowPrices[plateauStartIndex - 1] - peakLow) < tolerance)
            {
                plateauStartIndex--;
            }

            // Now check if the bars to the left of the plateau are all higher
            for (int i = 1; i <= swingCandles; i++)
            {
                int leftIndex = plateauStartIndex - i;
                if (leftIndex < 0 || series.LowPrices[leftIndex] <= peakLow + tolerance) // Must be clearly higher
                {
                    return false;
                }
            }

            // And check if the bars to the right of the original barIndex are all higher
            for (int i = 1; i <= swingCandles; i++)
            {
                if (barIndex + i >= series.Count || series.LowPrices[barIndex + i] < peakLow - tolerance) // Must be clearly higher, relaxed condition
                {
                    return false;
                }
            }

            return true;
        }

        private void IdentifyRelevantM1SwingForBOS(DateTime m1SweepBarTimeNY, SweepType lastSweepType)
        {
            var executionBars = MarketData.GetBars(_executionTimeFrame);
            if (executionBars.Count < 2) return;

            DateTime m1SweepBarOpenTimeServer;
            try
            {
                m1SweepBarOpenTimeServer = TimeZoneInfo.ConvertTime(m1SweepBarTimeNY, _newYorkTimeZone, TimeZone);
            }
            catch (ArgumentException ex)
            {
                Print($"Error converting M1 sweep bar time for GetIndexByTime: {ex.Message}. NYTime: {m1SweepBarTimeNY}, TargetZone: {TimeZone.DisplayName}");
                ResetSweepAndBosState($"Error converting sweep time: {ex.Message}");
                return;
            }
            
            int m1SweepBarIndex = executionBars.OpenTimes.GetIndexByTime(m1SweepBarOpenTimeServer);

            if (m1SweepBarIndex < 0)
            {
                Print($"Error: Could not find M1 bar for sweep time: {m1SweepBarTimeNY:yyyy-MM-dd HH:mm} NY. Cannot identify swing for BOS.");
                ResetSweepAndBosState("Could not find sweep bar index.");
                return;
            }
            
            // We look for the first swing pattern that forms *after* the sweep bar.
            // Loop from the bar after the sweep up to the second to last bar (as we need i+1).
            int loopStartIndex = m1SweepBarIndex + 1;
            // Process only confirmed bars. The last bar is still forming.
            int loopEndIndex = executionBars.Count - 2; 

            // Timeout: If no swing forms within X bars after sweep, reset.
            int swingSearchTimeoutBars = 30;
            if (loopEndIndex > m1SweepBarIndex + swingSearchTimeoutBars)
            {
                ResetSweepAndBosState($"No relevant swing pattern for BOS found within {swingSearchTimeoutBars} bars after the sweep at {m1SweepBarTimeNY:HH:mm}. Resetting state.");
                return;
            }

            if (lastSweepType == SweepType.LowSwept) // After Low sweep, we are bullish. Look for a 'high formation' to be broken.
            {
                for (int i = loopStartIndex; i <= loopEndIndex; i++)
                {
                    // High formation: bullish candle (i), then bearish candle (i+1)
                    bool isBullishCandle = executionBars.ClosePrices[i] > executionBars.OpenPrices[i];
                    bool isBearishCandleAfter = executionBars.ClosePrices[i+1] < executionBars.OpenPrices[i+1];

                    if (isBullishCandle && isBearishCandleAfter)
                    {
                        _relevantSwingLevelForBOS = Math.Max(executionBars.HighPrices[i], executionBars.HighPrices[i + 1]);
                        _relevantSwingBarTimeNY = GetNewYorkTime(executionBars.OpenTimes[i + 1]); // Time of the second candle in pattern
                        Print($"Relevant M1 Swing High for BOS identified at {Math.Round(_relevantSwingLevelForBOS, Symbol.Digits)} (Pattern End Bar: {_relevantSwingBarTimeNY:yyyy-MM-dd HH:mm} NY) after Low Sweep.");
                        DrawBosLevel(_relevantSwingLevelForBOS, _relevantSwingBarTimeNY);
                        return; // Found first one, exit
                    }
                }
            }
            else if (lastSweepType == SweepType.HighSwept) // After High sweep, we are bearish. Look for a 'low formation' to be broken.
            {
                for (int i = loopStartIndex; i <= loopEndIndex; i++)
                {
                    // Low formation: bearish candle (i), then bullish candle (i+1)
                    bool isBearishCandle = executionBars.ClosePrices[i] < executionBars.OpenPrices[i];
                    bool isBullishCandleAfter = executionBars.ClosePrices[i+1] > executionBars.OpenPrices[i+1];

                    if (isBearishCandle && isBullishCandleAfter)
                    {
                        _relevantSwingLevelForBOS = Math.Min(executionBars.LowPrices[i], executionBars.LowPrices[i + 1]);
                        _relevantSwingBarTimeNY = GetNewYorkTime(executionBars.OpenTimes[i + 1]); // Time of the second candle in pattern
                        Print($"Relevant M1 Swing Low for BOS identified at {Math.Round(_relevantSwingLevelForBOS, Symbol.Digits)} (Pattern End Bar: {_relevantSwingBarTimeNY:yyyy-MM-dd HH:mm} NY) after High Sweep.");
                        DrawBosLevel(_relevantSwingLevelForBOS, _relevantSwingBarTimeNY);
                        return; // Found first one, exit
                    }
                }
            }
            // No swing found yet, will try again on the next tick.
        }

        private void CheckForBOSAndFVG(DateTime currentTimeNY)
        {
            if (_lastSweepType == SweepType.None || _relevantSwingLevelForBOS == 0 || _sweepBarTimeNY == DateTime.MinValue)
            {
                return;
            }

            var executionBars = MarketData.GetBars(_executionTimeFrame);
            if (executionBars.Count < SwingCandles * 2 + 1) // Need enough bars for FVG and BOS checks
            {
                // Not enough bars to proceed, but don't reset state yet, maybe more bars will load.
                return;
            }

            DateTime sweepBarTimeUtc = TimeZoneInfo.ConvertTime(_sweepBarTimeNY, _newYorkTimeZone, TimeZoneInfo.Utc);
            // Find the execution bar index that is at or after the M1 sweep bar's open time
            int firstBarToCheckForBOSIndexOnExecutionTF = executionBars.OpenTimes.GetIndexByTime(sweepBarTimeUtc);
            if (firstBarToCheckForBOSIndexOnExecutionTF < 0)
            {
                Print($"Error finding execution bar index in CheckForBOSAndFVG for sweep bar time: {_sweepBarTimeNY:yyyy-MM-dd HH:mm} (UTC: {sweepBarTimeUtc}). Resetting.");
                ResetSweepAndBosState("Execution bar index not found for sweep time.");
                return;
            }
            
            // Loop for BOS candidate bar on execution timeframe (e.g., M1)
            // Start from the second to last bar, as the last bar is still forming.
            for (int bosCandidateBarIndex = executionBars.Count - 2; bosCandidateBarIndex > firstBarToCheckForBOSIndexOnExecutionTF; bosCandidateBarIndex--) 
            {
                DateTime bosCandidateBarOpenTimeNY = GetNewYorkTime(executionBars.OpenTimes[bosCandidateBarIndex]);
                // Ensure bosCandidateBarIndex is not before or at the swing bar time itself for BOS check.
                // _relevantSwingBarTimeNY is the open time of the swing bar.
                if (_relevantSwingBarTimeNY != DateTime.MinValue && bosCandidateBarOpenTimeNY <= _relevantSwingBarTimeNY) 
                    continue;

                var closePrice = executionBars.ClosePrices[bosCandidateBarIndex];
                var directionText = _lastSweepType == SweepType.HighSwept ? "Bearish" : "Bullish";
                var comparisonText = _lastSweepType == SweepType.HighSwept ? "<" : ">";
                Print($"BOS Check ({directionText}): Bar[{bosCandidateBarOpenTimeNY:HH:mm}] Close={closePrice} vs BOS Level={_relevantSwingLevelForBOS}. BOS Needed: Close {comparisonText} Level.");

                bool bosConfirmedThisBar = false;
                if (_lastSweepType == SweepType.HighSwept && closePrice < _relevantSwingLevelForBOS) 
                {
                    _bosLevel = _relevantSwingLevelForBOS; 
                    _bosTimeNY = bosCandidateBarOpenTimeNY; // M1 bar time
                    Print($"BEARISH BOS DETECTED on {_executionTimeFrame} at bar {_bosTimeNY:yyyy-MM-dd HH:mm} NY. Structure broken below Swing Low: {Math.Round(_relevantSwingLevelForBOS, Symbol.Digits)}. Sweep was: {_lastSweepType}");
                    bosConfirmedThisBar = true;
                    DrawM1BosConfirmationLine(GetNewYorkTime(executionBars.OpenTimes[bosCandidateBarIndex]), SweepType.HighSwept);
                }
                else if (_lastSweepType == SweepType.LowSwept && closePrice > _relevantSwingLevelForBOS) 
                {
                    _bosLevel = _relevantSwingLevelForBOS;
                    _bosTimeNY = bosCandidateBarOpenTimeNY; // M1 bar time
                    Print($"BULLISH BOS DETECTED on {_executionTimeFrame} at bar {_bosTimeNY:yyyy-MM-dd HH:mm} NY. Structure broken above Swing High: {Math.Round(_relevantSwingLevelForBOS, Symbol.Digits)}. Sweep was: {_lastSweepType}");
                    bosConfirmedThisBar = true;
                    DrawM1BosConfirmationLine(GetNewYorkTime(executionBars.OpenTimes[bosCandidateBarIndex]), SweepType.LowSwept);
                }

                if (bosConfirmedThisBar)
                {
                    _isProcessingSetup = true; // Lock the bot to process this one setup.

                    if (EntryStrategy == EntryStrategyType.OnBOS)
                    {
                        Print($"BOS confirmed at {_bosTimeNY:HH:mm}, preparing order based on {EntryStrategy} strategy.");
                        PrepareAndPlaceBOSEntryOrder(_lastSweepType);
                        return; // Exit. The order function is now responsible for state.
                    }
                    else // EntryStrategyType.OnFVG
                    {
                        Print($"BOS confirmed at {_bosTimeNY:HH:mm}, now searching for FVG based on {EntryStrategy} strategy.");
                        
                        bool fvgSetupFoundAndProcessing = false;
                    for (int fvgSearchIndex = bosCandidateBarIndex; fvgSearchIndex >= firstBarToCheckForBOSIndexOnExecutionTF; fvgSearchIndex--)
                    {
                        if (fvgSearchIndex < 1 || fvgSearchIndex + 1 >= executionBars.Count) continue;

                        double fvgLowBoundary, fvgHighBoundary;
                            bool fvgFoundThisIteration = false;

                        if (_lastSweepType == SweepType.HighSwept) // Bearish BOS, look for Bearish FVG
                        {
                            if (FindBearishFVG(fvgSearchIndex, executionBars, out fvgLowBoundary, out fvgHighBoundary))
                            {
                                Print($"Bearish FVG found on {_executionTimeFrame} based on bar {GetNewYorkTime(executionBars.OpenTimes[fvgSearchIndex]):yyyy-MM-dd HH:mm} NY. Range: {fvgLowBoundary}-{fvgHighBoundary}");
                                _lastFvgDetermined_Low = fvgLowBoundary;
                                _lastFvgDetermined_High = fvgHighBoundary;
                                _fvgBarN_OpenTimeNY = GetNewYorkTime(executionBars.OpenTimes[fvgSearchIndex - 1]);
                                _fvgBarN2_OpenTimeNY = GetNewYorkTime(executionBars.OpenTimes[fvgSearchIndex + 1]);
                                DrawFvgRectangle(_fvgBarN_OpenTimeNY, _lastFvgDetermined_High, _fvgBarN2_OpenTimeNY, _lastFvgDetermined_Low, _executionTimeFrame);                                
                                PrepareAndPlaceFVGEntryOrder(SweepType.HighSwept); 
                                    fvgFoundThisIteration = true;
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
                                    fvgFoundThisIteration = true;
                            }
                        }

                            if (fvgFoundThisIteration)
                        {
                                fvgSetupFoundAndProcessing = true;
                                break; // Exit FVG search loop once one is found and processed
                            }
                        }

                        if (!fvgSetupFoundAndProcessing)
                        {
                             Print($"BOS confirmed on {_executionTimeFrame} at {_bosTimeNY:yyyy-MM-dd HH:mm} NY, but NO FVG found in range from sweep bar to BOS bar on {_executionTimeFrame}. Resetting.");
                             ResetSweepAndBosState(""); // Quiet reset, this will also unlock the bot.
                        }
                        return; // Exit after first BOS confirmation is processed.
                    }
                }
            }
            
            // Timeout for BOS detection on execution timeframe
            int executionTimeFrameMinutes = GetTimeFrameInMinutes(_executionTimeFrame);
            if (executionTimeFrameMinutes == 0) executionTimeFrameMinutes = 1; // safety for division
            int bosTimeoutBars = (30 * GetTimeFrameInMinutes(_contextTimeFrame)) / executionTimeFrameMinutes; // This is now 30 M1 bars


            if (executionBars.Count -1 > firstBarToCheckForBOSIndexOnExecutionTF + bosTimeoutBars && firstBarToCheckForBOSIndexOnExecutionTF >=0) 
            {
                ResetSweepAndBosState($"No BOS detected on {_executionTimeFrame} within approx. {bosTimeoutBars} bars of sweep. Resetting sweep state for {_lastSweepType}. Sweep bar was {_sweepBarTimeNY:HH:mm}");
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

        private void PrepareAndPlaceFVGEntryOrder(SweepType bosDirection)
        {
            // Check daily trading limits before proceeding
            if (_tradesTakenToday >= 3)
            {
                Print("Daily trade limit (3) reached. No more trades today.");
                ResetSweepAndBosState("Daily trade limit reached.");
                return;
            }

            if (_dailyProfitTargetMet)
            {
                Print("Daily profit target (>=1%) met. No more trades today.");
                ResetSweepAndBosState("Daily profit target met.");
                return;
            }

            if (_sweepBarTimeNY == DateTime.MinValue || _bosTimeNY == DateTime.MinValue)
            {
                Print("Error: Sweep bar time or BOS time for SL not set. Order not placed.");
                ResetSweepAndBosState("Sweep/BOS details missing for FVG entry.");
                return;
            }

            if (!string.IsNullOrEmpty(_currentPendingOrderLabel))
            {
                var existingOrder = PendingOrders.FirstOrDefault(o => o.Label == _currentPendingOrderLabel);
                if (existingOrder != null)
                {
                    CancelPendingOrderAsync(existingOrder, cr => 
                    {
                        if (cr.IsSuccessful) Print($"Previous pending order {existingOrder.Label} cancelled for new FVG entry.");
                        else Print($"Failed to cancel previous pending order {existingOrder.Label}: {cr.Error}");
                    });
                    _currentPendingOrderLabel = null; 
                }
            }

            var executionBars = MarketData.GetBars(_executionTimeFrame);
            DateTime sweepBarTimeServer;
            DateTime bosBarTimeServer;
            try
            {
                sweepBarTimeServer = TimeZoneInfo.ConvertTime(_sweepBarTimeNY, _newYorkTimeZone, TimeZone);
                bosBarTimeServer = TimeZoneInfo.ConvertTime(_bosTimeNY, _newYorkTimeZone, TimeZone);
            }
            catch (Exception ex)
            {
                Print($"Error converting sweep/BOS bar time for SL calc: {ex.Message}. Order cancelled.");
                ResetSweepAndBosState("Time conversion error for sweep bar.");
                return;
            }
            int sweepBarIndex = executionBars.OpenTimes.GetIndexByTime(sweepBarTimeServer);
            int bosBarIndex = executionBars.OpenTimes.GetIndexByTime(bosBarTimeServer);

            if (sweepBarIndex < 0 || bosBarIndex < 0)
            {
                Print($"Error: Could not find sweep bar index ({_sweepBarTimeNY:HH:mm} NY) or BOS bar index ({_bosTimeNY:HH:mm} NY) for SL calculation. Order not placed.");
                ResetSweepAndBosState("Sweep/BOS bar index not found for FVG entry SL.");
                return;
            }

            double stopLossPrice;
            if (bosDirection == SweepType.LowSwept) // Bullish, find lowest low for SL
            {
                double lowestLow = double.MaxValue;
                for (int i = sweepBarIndex; i <= bosBarIndex; i++)
                {
                    if (executionBars.LowPrices[i] < lowestLow)
                    {
                        lowestLow = executionBars.LowPrices[i];
                    }
                }
                stopLossPrice = lowestLow;
                Print($"SL for Bullish FVG setup calculated as the lowest low between sweep bar {_sweepBarTimeNY:HH:mm} and BOS bar {_bosTimeNY:HH:mm}. SL Price: {stopLossPrice}");
            }
            else // Bearish, find highest high for SL
            {
                double highestHigh = 0;
                for (int i = sweepBarIndex; i <= bosBarIndex; i++)
                {
                    if (executionBars.HighPrices[i] > highestHigh)
                    {
                        highestHigh = executionBars.HighPrices[i];
                    }
                }
                stopLossPrice = highestHigh;
                Print($"SL for Bearish FVG setup calculated as the highest high between sweep bar {_sweepBarTimeNY:HH:mm} and BOS bar {_bosTimeNY:HH:mm}. SL Price: {stopLossPrice}");
            }

            double entryPrice, takeProfitPrice;
            TradeType tradeType;
            string newOrderLabel = $"SB_FVG_{SymbolName}_{Server.Time:yyyyMMddHHmmss}";

            if (bosDirection == SweepType.LowSwept) // Bullish BOS after LowSweep
            {
                tradeType = TradeType.Buy;
                entryPrice = _lastFvgDetermined_High; // Entry at the top of Bullish FVG
                Print($"FVG Entry Logic: Buy Limit at top of Bullish FVG {_lastFvgDetermined_High}, SL based on lowest low since sweep: {stopLossPrice}");
            }
            else // Bearish BOS after HighSwept
            {
                tradeType = TradeType.Sell;
                entryPrice = _lastFvgDetermined_Low; // Entry at the bottom of Bearish FVG
                Print($"FVG Entry Logic: Sell Limit at bottom of Bearish FVG {_lastFvgDetermined_Low}, SL based on highest high since sweep: {stopLossPrice}");
            }

            if ((tradeType == TradeType.Buy && entryPrice <= stopLossPrice) || (tradeType == TradeType.Sell && entryPrice >= stopLossPrice))
            {
                Print($"Invalid SL/Entry for FVG trade: Entry {entryPrice}, SL {stopLossPrice} for {tradeType}. Order not placed.");
                ResetSweepAndBosState("Invalid SL/Entry for FVG entry.");
                return;
            }
            
            takeProfitPrice = FindTakeProfitLevel(tradeType, entryPrice, stopLossPrice);
            if (takeProfitPrice == 0)
            {
                Print($"No suitable Take Profit target found meeting Min RR >= {MinRiskRewardRatio} for FVG entry. Order not placed.");
                ResetSweepAndBosState("Order cancelled due to no valid TP target for FVG entry.");
                return;
            }

            double volume = CalculateOrderVolume(Math.Abs(entryPrice - stopLossPrice) / Symbol.PipSize);
            if (volume == 0)
            {
                Print("Calculated volume is zero for FVG entry. Order not placed.");
                ResetSweepAndBosState("Volume is zero for FVG entry.");
                return;
            }

            if ((tradeType == TradeType.Buy && entryPrice >= Symbol.Ask) || (tradeType == TradeType.Sell && entryPrice <= Symbol.Bid))
            {
                Print($"FVG Limit entry {entryPrice} is on the wrong side of current market price. It would fill immediately. Order not placed to avoid slippage.");
                ResetSweepAndBosState("FVG entry price already passed by market.");
                return;
            }

            Print($"Preparing to place {tradeType} Limit Order (FVG Strategy): Label={newOrderLabel}, Vol={volume}, Entry={Math.Round(entryPrice, Symbol.Digits)}, SL={Math.Round(stopLossPrice, Symbol.Digits)}, TP={Math.Round(takeProfitPrice, Symbol.Digits)}");
            
            PlaceLimitOrderAsync(tradeType, SymbolName, volume, entryPrice, newOrderLabel, stopLossPrice, takeProfitPrice, (ProtectionType?)null, tradeResult =>
            {
                if (tradeResult.IsSuccessful)
                {
                    Print($"Successfully placed pending order (FVG) {newOrderLabel}.");
                    _pendingOrderInfo[newOrderLabel] = new Tuple<double, double>(stopLossPrice, takeProfitPrice);
                    DrawPendingOrderLines(entryPrice, stopLossPrice, takeProfitPrice);
                    _currentPendingOrderLabel = newOrderLabel;
            }
            else
            {
                    Print($"Failed to place pending order (FVG) {newOrderLabel}: {tradeResult.Error}.");
                    ResetSweepAndBosState($"Failed to place FVG order: {tradeResult.Error}");
                }
            });
        }

        private void PrepareAndPlaceBOSEntryOrder(SweepType bosDirection)
        {
            // Check daily trading limits before proceeding
            if (_tradesTakenToday >= 3)
            {
                Print("Daily trade limit (3) reached. No more trades today.");
                ResetSweepAndBosState("Daily trade limit reached.");
                return;
            }

            if (_dailyProfitTargetMet)
            {
                Print("Daily profit target (>=1%) met. No more trades today.");
                ResetSweepAndBosState("Daily profit target met.");
                return;
            }

            if (_sweepBarTimeNY == DateTime.MinValue || _bosTimeNY == DateTime.MinValue)
            {
                Print("Error: Sweep bar time or BOS time for SL not set. Order not placed.");
                ResetSweepAndBosState("Sweep/BOS details missing for BOS market entry.");
                return;
            }

            // Cancel any existing pending orders from other strategies if they exist
            if (!string.IsNullOrEmpty(_currentPendingOrderLabel))
            {
                var existingOrder = PendingOrders.FirstOrDefault(o => o.Label == _currentPendingOrderLabel);
                if (existingOrder != null)
                {
                    CancelPendingOrderAsync(existingOrder, cr =>
                    {
                        if (cr.IsSuccessful) Print($"Previous pending order {existingOrder.Label} cancelled for new BOS market entry.");
                        else Print($"Failed to cancel previous pending order {existingOrder.Label}: {cr.Error}");
                    });
                    _currentPendingOrderLabel = null;
                }
            }

            var executionBars = MarketData.GetBars(_executionTimeFrame);
            DateTime sweepBarTimeServer;
            DateTime bosBarTimeServer;
            try
            {
                sweepBarTimeServer = TimeZoneInfo.ConvertTime(_sweepBarTimeNY, _newYorkTimeZone, TimeZone);
                bosBarTimeServer = TimeZoneInfo.ConvertTime(_bosTimeNY, _newYorkTimeZone, TimeZone);
            }
            catch (Exception ex)
            {
                Print($"Error converting sweep/BOS bar time for SL calc: {ex.Message}. Order cancelled.");
                ResetSweepAndBosState("Time conversion error for sweep bar.");
                return;
            }
            int sweepBarIndex = executionBars.OpenTimes.GetIndexByTime(sweepBarTimeServer);
            int bosBarIndex = executionBars.OpenTimes.GetIndexByTime(bosBarTimeServer);

            if (sweepBarIndex < 0 || bosBarIndex < 0)
            {
                Print($"Error: Could not find sweep bar index ({_sweepBarTimeNY:HH:mm} NY) or BOS bar index ({_bosTimeNY:HH:mm} NY) for SL calculation. Order not placed.");
                ResetSweepAndBosState("Sweep/BOS bar index not found for BOS entry SL.");
                return;
            }

            double stopLossPrice;
            if (bosDirection == SweepType.LowSwept) // Bullish, find lowest low for SL
            {
                double lowestLow = double.MaxValue;
                for (int i = sweepBarIndex; i <= bosBarIndex; i++)
                {
                    if (executionBars.LowPrices[i] < lowestLow) lowestLow = executionBars.LowPrices[i];
                }
                stopLossPrice = lowestLow;
                Print($"SL for Bullish BOS calculated as the lowest low between sweep bar {_sweepBarTimeNY:HH:mm} and BOS bar {_bosTimeNY:HH:mm}. SL Price: {stopLossPrice}");
            }
            else // Bearish, find highest high for SL
            {
                double highestHigh = 0;
                for (int i = sweepBarIndex; i <= bosBarIndex; i++)
                {
                    if (executionBars.HighPrices[i] > highestHigh) highestHigh = executionBars.HighPrices[i];
                }
                stopLossPrice = highestHigh;
                Print($"SL for Bearish BOS calculated as the highest high between sweep bar {_sweepBarTimeNY:HH:mm} and BOS bar {_bosTimeNY:HH:mm}. SL Price: {stopLossPrice}");
            }

            double entryPrice;
            double takeProfitPrice;
            TradeType tradeType;
            string newOrderLabel = $"SB_BOS_MKT_{SymbolName}_{Server.Time:yyyyMMddHHmmss}";

            if (bosDirection == SweepType.LowSwept) // Bullish BOS after LowSweep
            {
                tradeType = TradeType.Buy;
                entryPrice = Symbol.Ask; // Current market price for entry
                Print($"BOS Entry Logic: Buy Market, SL based on lowest low since sweep: {stopLossPrice}");
            }
            else // Bearish BOS after HighSwept
            {
                tradeType = TradeType.Sell;
                entryPrice = Symbol.Bid; // Current market price for entry
                Print($"BOS Entry Logic: Sell Market, SL based on highest high since sweep: {stopLossPrice}");
            }

            if ((tradeType == TradeType.Buy && entryPrice <= stopLossPrice) || (tradeType == TradeType.Sell && entryPrice >= stopLossPrice))
            {
                Print($"Invalid SL/Entry for BOS market trade: Entry ~{entryPrice}, SL {stopLossPrice} for {tradeType}. Order not placed.");
                ResetSweepAndBosState("Invalid SL/Entry for BOS market entry.");
                return;
            }

            double stopLossInPips = Math.Abs(entryPrice - stopLossPrice) / Symbol.PipSize;
            if (stopLossInPips <= 0.5)
            {
                Print($"Stop loss is too small ({stopLossInPips} pips for BOS market entry). Min SL: 0.5 pips. Order not placed.");
                ResetSweepAndBosState("SL too small for BOS market entry.");
                return;
            }

            takeProfitPrice = FindTakeProfitLevel(tradeType, entryPrice, stopLossPrice);
            if (takeProfitPrice == 0)
            {
                Print($"No suitable Take Profit target found meeting Min RR >= {MinRiskRewardRatio} for BOS market entry. Order not placed.");
                ResetSweepAndBosState("Order cancelled due to no valid TP target for BOS market entry.");
                return;
            }

            double volume = CalculateOrderVolume(stopLossInPips);
            if (volume == 0)
            {
                Print("Calculated volume is zero for BOS market entry. Order not placed.");
                ResetSweepAndBosState("Volume is zero for BOS market entry.");
                return;
            }

            Print($"Preparing to place {tradeType} Market Order (BOS Strategy): Label={newOrderLabel}, Vol={volume}, SL={Math.Round(stopLossPrice, Symbol.Digits)}, TP={Math.Round(takeProfitPrice, Symbol.Digits)}");

            ExecuteMarketOrderAsync(tradeType, SymbolName, volume, newOrderLabel, null, null, tradeResult =>
            {
                if (tradeResult.IsSuccessful)
                {
                    Print($"Successfully placed market order (BOS) {newOrderLabel}. Position: {tradeResult.Position.Id}");
                    var position = tradeResult.Position;

                    var slResult = position.ModifyStopLossPrice(stopLossPrice);
                    if (slResult.IsSuccessful)
                    {
                        Print($"Successfully set SL for position {position.Id} to {stopLossPrice}.");
                    }
                    else
                    {
                        Print($"Failed to set SL for position {position.Id}: {slResult.Error}");
                    }

                    var tpResult = position.ModifyTakeProfitPrice(takeProfitPrice);
                    if (tpResult.IsSuccessful)
                    {
                        Print($"Successfully set TP for position {position.Id} to {takeProfitPrice}.");
                    }
                    else
                    {
                        Print($"Failed to set TP for position {position.Id}: {tpResult.Error}");
                    }
                    
                    // OnPositionOpened will handle _tradesTakenToday++
                    // Reset state immediately since market order is filled or failed.
                    ResetSweepAndBosState($"Market order {newOrderLabel} processed, resetting state.");
                }
                else
                {
                    Print($"Failed to place market order (BOS) {newOrderLabel}: {tradeResult.Error}.");
                    ResetSweepAndBosState($"Failed to place BOS market order: {tradeResult.Error}");
                }
            });
        }

        private void OnPendingOrderFilled(PendingOrderFilledEventArgs args)
        {
            var orderLabel = args.PendingOrder.Label;
            Print($"Pending order {orderLabel} filled and became position {args.Position.Id} (Symbol: {args.Position.SymbolName}, Type: {args.Position.TradeType}, Volume: {args.Position.VolumeInUnits}, Entry: {args.Position.EntryPrice}).");

            if (_pendingOrderInfo.TryGetValue(orderLabel, out var slTpInfo))
            {
                var position = args.Position;
                var slPrice = slTpInfo.Item1;
                var tpPrice = slTpInfo.Item2;

                if (slPrice != 0)
                {
                    var slResult = position.ModifyStopLossPrice(slPrice);
                    if (slResult.IsSuccessful)
                    {
                        Print($"Successfully modified SL for position {position.Id} to {slPrice}.");
                    }
                    else
                    {
                        Print($"Failed to modify SL for position {position.Id}: {slResult.Error}");
                    }
                }
                if (tpPrice != 0)
                {
                    var tpResult = position.ModifyTakeProfitPrice(tpPrice);
                    if (tpResult.IsSuccessful)
                    {
                        Print($"Successfully modified TP for position {position.Id} to {tpPrice}.");
                    }
                    else
                    {
                        Print($"Failed to modify TP for position {position.Id}: {tpResult.Error}");
                    }
                }
                
                _pendingOrderInfo.Remove(orderLabel);
                if (orderLabel == _currentPendingOrderLabel)
                {
                    _currentPendingOrderLabel = null; 
                }
                ClearPendingOrderLines();
                ResetSweepAndBosState($"Position {position.Id} opened and modified from order {orderLabel}, resetting state.");
            }
        }

        private void OnPendingOrderCancelled(PendingOrderCancelledEventArgs args)
        {
            Print($"Pending order {args.PendingOrder.Label} was cancelled. Reason: {args.Reason}");
            var orderLabel = args.PendingOrder.Label;

            if (orderLabel == _currentPendingOrderLabel)
            {
                _currentPendingOrderLabel = null;
                ClearPendingOrderLines();
                ResetSweepAndBosState($"Pending order {orderLabel} cancelled, resetting state.");
            }
            
            if (_pendingOrderInfo.ContainsKey(orderLabel))
            {
                _pendingOrderInfo.Remove(orderLabel);
            }
        }

        private void OnPositionOpened(PositionOpenedEventArgs args)
        { 
            Print($"Position {args.Position.Id} opened (Symbol: {args.Position.SymbolName}, Type: {args.Position.TradeType}, Volume: {args.Position.VolumeInUnits}, Entry: {args.Position.EntryPrice}, SL: {args.Position.StopLoss}, TP: {args.Position.TakeProfit}).");
            _tradesTakenToday++;
            Print($"Trade count for today: {_tradesTakenToday}.");
            
            // Resetting state is now handled in OnPendingOrderFilled to avoid race conditions.
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

            // Clear drawings
            ClearBosAndFvgDrawings(); // Clears BosSwingLineName and FvgRectName
            Chart.RemoveObject(M1BosConfirmationLineName);
            Chart.RemoveObject(M1BosConfirmationTextName);

            // Crucially, unlock the bot to allow searching for new setups
            _isProcessingSetup = false;
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
            return 1; // Defaulting to 1 minute as a safer fallback.
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

        private void DrawLiquidityLevels(List<Tuple<double, DateTime>> highs, List<Tuple<double, DateTime>> lows, DateTime sessionStartTimeNY)
        {
            ClearLiquidityDrawings();

            for (int i = 0; i < Math.Min(highs.Count, MaxLiquidityLevelsToDraw); i++)
            {
                var high = highs[i].Item1;
                var highTime = highs[i].Item2;
                if (high != 0 && highTime != DateTime.MinValue)
                {
                    DateTime startTimeServer = TimeZoneInfo.ConvertTime(highTime, _newYorkTimeZone, TimeZone);
                    DateTime endTimeServer = TimeZoneInfo.ConvertTime(sessionStartTimeNY, _newYorkTimeZone, TimeZone);
                    Chart.DrawTrendLine(LiqHighLineName + i, startTimeServer, high, endTimeServer, high, Color.Blue, 2, LineStyle.Solid);
                    Chart.DrawText(LiqHighTextName + i, "+", endTimeServer, high, Color.Blue).VerticalAlignment = VerticalAlignment.Bottom;
                }
            }

            for (int i = 0; i < Math.Min(lows.Count, MaxLiquidityLevelsToDraw); i++)
            {
                var low = lows[i].Item1;
                var lowTime = lows[i].Item2;
                if (low != 0 && lowTime != DateTime.MinValue)
                {
                    DateTime startTimeServer = TimeZoneInfo.ConvertTime(lowTime, _newYorkTimeZone, TimeZone);
                    DateTime endTimeServer = TimeZoneInfo.ConvertTime(sessionStartTimeNY, _newYorkTimeZone, TimeZone);
                    Chart.DrawTrendLine(LiqLowLineName + i, startTimeServer, low, endTimeServer, low, Color.Red, 2, LineStyle.Solid);
                    Chart.DrawText(LiqLowTextName + i, "-", endTimeServer, low, Color.Red).VerticalAlignment = VerticalAlignment.Bottom;
                }
            }
        }

        private void DrawBosLevel(double level, DateTime swingBarOpenTimeNY)
        {
            Chart.RemoveObject(BosSwingLineName);
            Chart.RemoveObject(BosLevelTextName);
            if (level != 0 && swingBarOpenTimeNY != DateTime.MinValue)
            {
                DateTime startTimeServer = TimeZoneInfo.ConvertTime(swingBarOpenTimeNY, _newYorkTimeZone, TimeZone);
                // Extend the line for a fixed number of execution bars (e.g., 10 bars)
                TimeSpan executionBarDuration = GetTimeFrameTimeSpan(_executionTimeFrame);
                DateTime endTimeServer = startTimeServer.Add(TimeSpan.FromTicks(executionBarDuration.Ticks * 10)); 

                Chart.DrawTrendLine(BosSwingLineName, startTimeServer, level, endTimeServer, level, Color.Orange, 2, LineStyle.Dots);
                Chart.DrawText(BosLevelTextName, "BOS Level", endTimeServer, level, Color.Orange).VerticalAlignment = VerticalAlignment.Bottom;
            }
        }

        private void DrawFvgRectangle(DateTime startTimeNY, double topPrice, DateTime endTimeNY, double bottomPrice, TimeFrame frameForDuration)
        {
            Chart.RemoveObject(FvgRectName);
            Chart.RemoveObject(FvgTextName);
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
                Chart.DrawText(FvgTextName, "FVG", endTimeServer.Add(barDuration), rectTop, Color.Gray).VerticalAlignment = VerticalAlignment.Bottom;
            }
        }

        private void DrawPendingOrderLines(double entry, double sl, double tp)
        {
            ClearPendingOrderLines();
            var executionBars = MarketData.GetBars(_executionTimeFrame);
            if (executionBars.Count == 0) return;
            var lastBarTime = executionBars.Last().OpenTime;
            
            if (entry != 0)
            {
                Chart.DrawHorizontalLine(PendingEntryLineName, entry, Color.Green, 2, LineStyle.Dots);
                Chart.DrawText(PendingEntryTextName, "Entry", lastBarTime, entry, Color.Green).VerticalAlignment = VerticalAlignment.Bottom;
            }

            if (sl != 0)
            {
                Chart.DrawHorizontalLine(PendingSlLineName, sl, Color.Red, 2, LineStyle.Dots);
                Chart.DrawText(PendingSlTextName, "SL", lastBarTime, sl, Color.Red).VerticalAlignment = VerticalAlignment.Bottom;
            }

            if (tp != 0)
            {
                Chart.DrawHorizontalLine(PendingTpLineName, tp, Color.DodgerBlue, 2, LineStyle.Dots);
                Chart.DrawText(PendingTpTextName, "TP", lastBarTime, tp, Color.DodgerBlue).VerticalAlignment = VerticalAlignment.Bottom;
            }
        }

        private void ClearLiquidityDrawings()
        {
            for (int i = 0; i < MaxLiquidityLevelsToDraw; i++)
            {
                Chart.RemoveObject(LiqHighLineName + i);
                Chart.RemoveObject(LiqLowLineName + i);
                Chart.RemoveObject(LiqHighTextName + i);
                Chart.RemoveObject(LiqLowTextName + i);
            }
        }

        private void ClearBosAndFvgDrawings()
        {
            Chart.RemoveObject(BosSwingLineName);
            Chart.RemoveObject(FvgRectName);
            Chart.RemoveObject(M1BosConfirmationLineName);
            Chart.RemoveObject(SweepLineName);
            Chart.RemoveObject(BosLevelTextName);
            Chart.RemoveObject(FvgTextName);
            Chart.RemoveObject(SweepTextName);
            Chart.RemoveObject(M1BosConfirmationTextName);
        }

        private void ClearPendingOrderLines()
        {
            Chart.RemoveObject(PendingEntryLineName);
            Chart.RemoveObject(PendingSlLineName);
            Chart.RemoveObject(PendingTpLineName);
            Chart.RemoveObject(PendingEntryTextName);
            Chart.RemoveObject(PendingSlTextName);
            Chart.RemoveObject(PendingTpTextName);
        }

        private void ClearSessionTimeLines()
        {
            for (int i = 1; i <= 3; i++)
            {
                Chart.RemoveObject($"Session{i}_Rect");
                Chart.RemoveObject($"Session{i}_StartLine");
                Chart.RemoveObject($"Session{i}_EndLine");
            }
        }

        private void ClearAllStrategyDrawings()
        {
            ClearLiquidityDrawings();
            ClearBosAndFvgDrawings();
            ClearPendingOrderLines();
            ClearSessionTimeLines();
        }

        private void DrawM1BosConfirmationLine(DateTime bosConfirmationBarTimeNY, SweepType originalSweepDirection)
        {
            Chart.RemoveObject(M1BosConfirmationLineName); // Remove previous one, if any
            Chart.RemoveObject(M1BosConfirmationTextName);
            DateTime serverTime = TimeZoneInfo.ConvertTime(bosConfirmationBarTimeNY, _newYorkTimeZone, TimeZone);
            Color lineColor;
            // The color depends on the direction of the BOS, which is opposite to the initial sweep for the swing identification,
            // but same direction logic as _lastSweepType leading to the BOS check.
            if (originalSweepDirection == SweepType.LowSwept) // Original M1 sweep was low, so M1 BOS is UP (Bullish)
            {
                lineColor = Color.DarkGreen;
            }
            else // Original M1 sweep was high, so M1 BOS is DOWN (Bearish)
            {
                lineColor = Color.DarkRed;
            }
            Chart.DrawVerticalLine(M1BosConfirmationLineName, serverTime, lineColor, 2, LineStyle.Solid);
            var text = originalSweepDirection == SweepType.LowSwept ? "Bull BOS" : "Bear BOS";
            Chart.DrawText(M1BosConfirmationTextName, text, serverTime, Chart.TopY, lineColor).VerticalAlignment = VerticalAlignment.Top;
        }

        private void DrawSweepLine(DateTime startTimeNY, double startPrice, DateTime endTimeNY, double endPrice)
        {
            Chart.RemoveObject(SweepLineName);
            Chart.RemoveObject(SweepTextName);
            
        }

        private void DrawSessionTimeLines()
        {
            var lineColor = Color.FromArgb(100, Color.Black); // Transparent black

            if (_session1ActualStartNY != DateTime.MinValue)
            {
                DateTime startTimeServer = TimeZoneInfo.ConvertTime(_session1ActualStartNY, _newYorkTimeZone, TimeZone);
                DateTime endTimeServer = TimeZoneInfo.ConvertTime(_session1ActualEndNY, _newYorkTimeZone, TimeZone);
                Chart.DrawVerticalLine("Session1_StartLine", startTimeServer, lineColor, 1, LineStyle.Solid);
                Chart.DrawVerticalLine("Session1_EndLine", endTimeServer, lineColor, 1, LineStyle.Solid);
            }
            if (_session2ActualStartNY != DateTime.MinValue)
            {
                DateTime startTimeServer = TimeZoneInfo.ConvertTime(_session2ActualStartNY, _newYorkTimeZone, TimeZone);
                DateTime endTimeServer = TimeZoneInfo.ConvertTime(_session2ActualEndNY, _newYorkTimeZone, TimeZone);
                Chart.DrawVerticalLine("Session2_StartLine", startTimeServer, lineColor, 1, LineStyle.Solid);
                Chart.DrawVerticalLine("Session2_EndLine", endTimeServer, lineColor, 1, LineStyle.Solid);
            }
            if (_session3ActualStartNY != DateTime.MinValue)
            {
                DateTime startTimeServer = TimeZoneInfo.ConvertTime(_session3ActualStartNY, _newYorkTimeZone, TimeZone);
                DateTime endTimeServer = TimeZoneInfo.ConvertTime(_session3ActualEndNY, _newYorkTimeZone, TimeZone);
                Chart.DrawVerticalLine("Session3_StartLine", startTimeServer, lineColor, 1, LineStyle.Solid);
                Chart.DrawVerticalLine("Session3_EndLine", endTimeServer, lineColor, 1, LineStyle.Solid);
            }
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
            _sweepBarTimeNY = DateTime.MinValue;
            
            _bosLevel = 0;
            _bosTimeNY = DateTime.MinValue;
            _fvgFoundOnBarOpenTimeNY = DateTime.MinValue;
            _lastFvgDetermined_High = 0;
            _lastFvgDetermined_Low = 0;
            _fvgBarN_OpenTimeNY = DateTime.MinValue;
            _fvgBarN2_OpenTimeNY = DateTime.MinValue;

            _tpTargetLiquidityLevels?.Clear();

            // Clear drawings
            ClearBosAndFvgDrawings(); // Clears BosSwingLineName and FvgRectName
            Chart.RemoveObject(M1BosConfirmationLineName);
            // Note: Liquidity lines (LiqHighLineName, LiqLowLineName) are managed by DailyReset or when new liquidity is identified.
            // M1 Sweep visualization lines are managed by VisualizeM3Sweeps itself (cleared on each run).
            _isProcessingSetup = false;
        }

        private double FindTakeProfitLevel(TradeType tradeType, double entryPrice, double stopLossPrice)
        {
            double stopLossPips = Math.Abs(entryPrice - stopLossPrice) / Symbol.PipSize;
            if (stopLossPips <= 0.1) 
            {
                Print($"Stop loss ({stopLossPips} pips) is too small for TP calculation. Aborting.");
                return 0;
            }

            double bestTarget = 0;
            double bestTargetRR = 0;
            bool hasPotentialTargets = _tpTargetLiquidityLevels != null && _tpTargetLiquidityLevels.Count > 0;

            if (hasPotentialTargets)
            {
                Print($"Searching for TP among {_tpTargetLiquidityLevels.Count} potential liquidity targets. SL pips: {stopLossPips:F2}. Entry: {entryPrice}.");

                foreach (var targetLevel in _tpTargetLiquidityLevels)
                {
                    double potentialTarget = targetLevel.Item1;
                    
                    if (tradeType == TradeType.Buy)
                    {
                        if (potentialTarget > entryPrice)
                        {
                            double targetPips = (potentialTarget - entryPrice) / Symbol.PipSize;
                            if (targetPips <= 0) continue;

                            double currentRR = targetPips / stopLossPips;
                            Print($"  - Checking target {potentialTarget}. RR: {currentRR:F2}. Required range: [{MinRiskRewardRatio} - {MaxRiskRewardRatio}]");
                            
                            if (currentRR >= MinRiskRewardRatio && currentRR <= MaxRiskRewardRatio)
                            {
                                if (bestTarget == 0 || potentialTarget > bestTarget) // Find furthest valid target (highest price for buy)
                                {
                                    bestTarget = potentialTarget;
                                    bestTargetRR = currentRR;
                                    Print($"    - >>> New Best TP candidate found: {bestTarget} with RR {bestTargetRR:F2}");
                                }
                            }
                        }
                    }
                    else // Sell Trade
                    {
                        if (potentialTarget < entryPrice)
                        {
                            double targetPips = (entryPrice - potentialTarget) / Symbol.PipSize;
                            if (targetPips <= 0) continue;
                            
                            double currentRR = targetPips / stopLossPips;
                            Print($"  - Checking target {potentialTarget}. RR: {currentRR:F2}. Required range: [{MinRiskRewardRatio} - {MaxRiskRewardRatio}]");

                            if (currentRR >= MinRiskRewardRatio && currentRR <= MaxRiskRewardRatio)
                            {
                                if (bestTarget == 0 || potentialTarget < bestTarget) // Find furthest valid target (lowest price for sell)
                                {
                                    bestTarget = potentialTarget;
                                    bestTargetRR = currentRR;
                                    Print($"    - >>> New Best TP candidate found: {bestTarget} with RR {bestTargetRR:F2}");
                                }
                            }
                        }
                    }
                }
            }

            if (bestTarget != 0)
            {
                Print($"Found best TP target at liquidity level {bestTarget} with RR {bestTargetRR:F2}.");
                return bestTarget;
            }
            
            Print($"No liquidity targets found that meet the required RR range [{MinRiskRewardRatio} - {MaxRiskRewardRatio}]. Aborting trade.");
            return 0;
        }
    }
}


