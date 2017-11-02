-- This script is useful to look at the effect of 'streaming' the rows
exec sp_execute_external_script @language = N'R',
@script = N'
stream <- stream + 1

OutputDataSet <- data.frame(RProcessId = Sys.getpid(), StreamId = stream, NumRows = nrow(InputDataSet))
'
, @input_data_1 = N'
SELECT   CASE WHEN OBS.USAF = 999999 THEN CONCAT(''WBAN_'', OBS.WBAN) ELSE CONCAT(''USAF_'', OBS.USAF) END AS StationKey,
         ObsMonth,
         ObsHour,
         DATEPART(dy, DATEFROMPARTS(ObsYear, ObsMonth, ObsDay)) AS DayOfYear,
         DATEFROMPARTS(ObsYear, ObsMonth, ObsDay) AS FullDate,
         AirTemp
FROM     Observations AS OBS WITH (NOLOCK)
         INNER JOIN
         [isd-history] AS ISD WITH (NOLOCK)
         ON OBS.USAF = ISD.USAF
            AND OBS.WBAN = ISD.WBAN
WHERE    ISD.State = ''WA''
         AND ISD.CTRY = ''US''
         AND ISD.USAF = 727930
         AND DATETIMEFROMPARTS(ObsYear, ObsMonth, ObsDay, ObsHour, 0, 0, 0) BETWEEN ''2007-01-01'' AND ''2017-10-29''
         AND AirTemp > -500  -- outliers
ORDER BY DATETIMEFROMPARTS(ObsYear, ObsMonth, ObsDay, ObsHour, 0, 0, 0);
'
, @params = N'@r_rowsPerRead INT, @stream int'
, @r_rowsPerRead = 5000
, @stream = 0
WITH RESULT SETS (
(RProcessId INT, StreamId INT, NumRows BIGINT)
)