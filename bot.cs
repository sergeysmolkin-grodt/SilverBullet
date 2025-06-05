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
                _contextTimeFrame = TimeFrame.m15;
            }

            Print("Silver Bullet Bot Started.");
            Print($"Trading Sessions (NY Time): {Session1StartNY}-{Session1EndNY}, {Session2StartNY}-{Session2EndNY}, {Session3StartNY}-{Session3EndNY}");
            Print($"Liquidity lookback: {MinutesBeforeSessionForLiquidity} minutes before session start.");
            Print($"Risk per trade: {RiskPercentage}%, RR: {RiskRewardRatio}");
            Print($"Context TimeFrame: {_contextTimeFrame}");
        }

        protected override void OnTick()
        {
            var currentTimeNY = GetNewYorkTime(Server.Time);
            DailyResetAndSessionTimeUpdate(currentTimeNY);

            SessionInfo currentSession = GetCurrentSessionInfo(currentTimeNY);

            if (currentSession.IsActivePeriod)
            {
                bool relevantLiquidityFlag = GetLiquidityIdentifiedFlag(currentSession.SessionNumber);

                if (!relevantLiquidityFlag && currentTimeNY >= currentSession.ActualStartNY)
                {
                    IdentifyLiquidityLevels(currentSession.ActualStartNY, currentSession.SessionNumber);
                }

                // Re-fetch flag as IdentifyLiquidityLevels might have set it
                relevantLiquidityFlag = GetLiquidityIdentifiedFlag(currentSession.SessionNumber);

                if (relevantLiquidityFlag && (_currentLiquidityHigh != 0 || _currentLiquidityLow != 0))
                {
                    // Ensure we are within the trading window (actual start to actual end)
                    // or the pre-buffer if that's where sweeps are also checked.
                    // The IsActivePeriod already covers SessionStart-Buffer to SessionEnd.
                    // We need to ensure we don't check for sweeps of liquidity *before* it's meant to be formed for future sessions.
                    // This check is implicitly handled because _currentLiquidityHigh/Low are tied to the *current* or *just started* session's liquidity.
                    CheckForLiquiditySweep(currentTimeNY);
                }
            }
            else
            {
                // Not in any active period for any session.
                // DailyResetAndSessionTimeUpdate handles clearing flags for the next day.
                // Clear current liquidity levels if they are from a past session that has now ended.
                if (_currentLiquidityHigh != 0 || _currentLiquidityLow != 0)
                {
                    // This might clear liquidity prematurely if we are between sessions on the same day.
                    // Let's only clear if no session is active AND the flags indicate no current session's liquidity is loaded.
                    // Better: Rely on DailyReset for flags, and clear _currentLiquidityHigh/Low if all flags are false.
                    if (!_liquidityIdentifiedForSession1 && !_liquidityIdentifiedForSession2 && !_liquidityIdentifiedForSession3)
                    {
                         // Print("Outside all trading session processing windows, and no session liquidity active. Clearing levels.");
                        _currentLiquidityHigh = 0;
                        _currentLiquidityLow = 0;
                    }
                }
            }
        }

        protected override void OnStop()
        {
            Print("Silver Bullet Bot Stopped.");
        }

        private DateTime GetNewYorkTime(DateTime serverTime)
        {
            return TimeZoneInfo.ConvertTimeFromUtc(serverTime, _newYorkTimeZone);
        }

        private bool IsInTradingSession(DateTime currentTimeNY)
        {
            TimeSpan currentTimeOfDay = currentTimeNY.TimeOfDay;
            TimeSpan preSessionBuffer = TimeSpan.FromMinutes(MinutesBeforeSessionForLiquidity);

            // Parse session times - these are string, should use the DateTime versions for comparison
            // This method is effectively replaced by GetCurrentSessionInfo logic

            // Session 1
            TimeSpan s1Start = TimeSpan.Parse(Session1StartNY);
            TimeSpan s1End = TimeSpan.Parse(Session1EndNY);
            if (currentTimeOfDay >= s1Start.Subtract(preSessionBuffer) && currentTimeOfDay < s1End) return true;
            
            // Session 2
            TimeSpan s2Start = TimeSpan.Parse(Session2StartNY);
            TimeSpan s2End = TimeSpan.Parse(Session2EndNY);
            if (currentTimeOfDay >= s2Start.Subtract(preSessionBuffer) && currentTimeOfDay < s2End) return true;

            // Session 3
            TimeSpan s3Start = TimeSpan.Parse(Session3StartNY);
            TimeSpan s3End = TimeSpan.Parse(Session3EndNY);
            if (currentTimeOfDay >= s3Start.Subtract(preSessionBuffer) && currentTimeOfDay < s3End) return true;
            
            return false;
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
            var contextSeries = MarketData.GetSeries(_contextTimeFrame);
            if (contextSeries.Count == 0)
            {
                Print("Error: Context series is empty. Cannot identify liquidity.");
                return;
            }

            // Get the index of the bar that STARTS AT sessionActualStartNY
            int barStartingAtSessionIndex = contextSeries.OpenTimes.GetIndexByTime(sessionActualStartNY);
            
            // We need the bar that completed just BEFORE sessionActualStartNY
            int liquidityBarIndex = -1;

            if (barStartingAtSessionIndex == -1) // sessionActualStartNY is beyond loaded history
            {
                 // If no bar starts exactly at sessionActualStartNY (e.g., time is in the future of series)
                 // or if GetIndexByTime returns -1 because the time is outside the series range.
                 // We look for the last bar whose open time is < sessionActualStartNY.
                 // This usually means the last bar in the series if sessionActualStartNY is in the future of current bars.
                 // This case might occur if data hasn't caught up or time is too far.
                 // For safety, if GetIndexByTime doesn't give a clear starting point, iterate backwards.
                for (int i = contextSeries.Count - 1; i >=0; i--)
                {
                    if (GetNewYorkTime(contextSeries.OpenTime[i]) < sessionActualStartNY)
                    {
                        liquidityBarIndex = i;
                        break;
                    }
                }
            }
            else
            {
                 liquidityBarIndex = barStartingAtSessionIndex - 1;
            }


            if (liquidityBarIndex >= 0 && liquidityBarIndex < contextSeries.Count)
            {
                _currentLiquidityHigh = contextSeries.High[liquidityBarIndex];
                _currentLiquidityLow = contextSeries.Low[liquidityBarIndex];
                _currentLiquiditySourceBarTimeNY = GetNewYorkTime(contextSeries.OpenTime[liquidityBarIndex]);
                
                SetLiquidityIdentifiedFlag(sessionNumber, true);
                Print($"Session {sessionNumber} ({sessionActualStartNY:HH:mm} NY): Liquidity identified from bar {_currentLiquiditySourceBarTimeNY:yyyy-MM-dd HH:mm} NY. High: {_currentLiquidityHigh}, Low: {_currentLiquidityLow}");
            }
            else
            {
                Print($"Error for Session {sessionNumber} ({sessionActualStartNY:HH:mm} NY): Could not find suitable bar index ({liquidityBarIndex}) in context series (count: {contextSeries.Count}) to identify liquidity. sessionActualStartNY: {sessionActualStartNY}, barStartingAtSessionIndex: {barStartingAtSessionIndex}");
                // Potentially set flag true anyway to avoid re-trying constantly if data is missing
                // Or handle this state more gracefully. For now, it won't identify, and retry next tick if flag remains false.
            }
        }

        private void CheckForLiquiditySweep(DateTime currentTimeNY)
        {
            // Ensure there's valid liquidity to check against
            if (_currentLiquidityHigh == 0 && _currentLiquidityLow == 0) return;

            double currentAsk = Symbol.Ask;
            double currentBid = Symbol.Bid;

            bool highSwept = false;
            bool lowSwept = false;

            if (_currentLiquidityHigh != 0 && currentAsk > _currentLiquidityHigh)
            {
                Print($"HIGH LIQUIDITY SWEPT at {_currentLiquidityHigh}. Price: {currentAsk}, Time: {currentTimeNY:HH:mm:ss} NY");
                _currentLiquidityHigh = 0; // Mark as swept, prevent re-trigger
                highSwept = true;
            }

            if (_currentLiquidityLow != 0 && currentBid < _currentLiquidityLow)
            {
                Print($"LOW LIQUIDITY SWEPT at {_currentLiquidityLow}. Price: {currentBid}, Time: {currentTimeNY:HH:mm:ss} NY");
                _currentLiquidityLow = 0; // Mark as swept, prevent re-trigger
                lowSwept = true;
            }

            if (highSwept || lowSwept)
            {
                // TODO: Here we would trigger the next phase of the strategy:
                // "дождаться слома структуры с наличием имба (агрессивное движение должно быть) и входить на тесте имба"
                Print("Liquidity sweep detected. Next step: Monitor for BOS and Imbalance.");
            }
        }

        // Further methods for strategy logic will be added below
        // e.g., CheckLiquiditySweep, CheckBOS, CheckImbalance, PlaceOrder, ManageTrade etc.
    }
}
