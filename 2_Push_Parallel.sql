USE ISDLite
GO

-- This example shows how 'trivial parallelism' works in SPEES
-- We change the query a bit here to demonstrate the parallelism:
-- 1. We get all rows for the state of WA, not just for Seattle
-- 2. We force a CCI scan to have the QO generate a parallel plan
exec sp_execute_external_script @language = N'R',
@script = N'
stream = stream + 1
OutputDataSet <- data.frame(RProcessId = Sys.getpid(), StreamId = stream, NumRows = nrow(InputDataSet))
'
, @input_data_1 = N'
SELECT   CASE WHEN OBS.USAF = 999999 THEN CONCAT(''WBAN_'', OBS.WBAN) ELSE CONCAT(''USAF_'', OBS.USAF) END AS StationKey,
         ObsMonth,
         ObsHour,
         DATEPART(dy, DATEFROMPARTS(ObsYear, ObsMonth, ObsDay)) AS DayOfYear,
         DATEFROMPARTS(ObsYear, ObsMonth, ObsDay) AS FullDate,
         AirTemp
FROM     Observations AS OBS WITH (NOLOCK, INDEX = 0)
         INNER JOIN
         [isd-history] AS ISD WITH (NOLOCK)
         ON OBS.USAF = ISD.USAF
            AND OBS.WBAN = ISD.WBAN
WHERE    ISD.CTRY = ''US''
         AND DATETIMEFROMPARTS(ObsYear, ObsMonth, ObsDay, ObsHour, 0, 0, 0) BETWEEN ''2007-01-01'' AND ''2017-10-29''
         AND AirTemp > -500  -- outliers

'
, @parallel = 1
, @params = N'@stream INT, @r_rowsPerRead INT'
, @r_rowsPerRead = 100000
, @stream = 0
WITH RESULT SETS (
(RProcessId INT, StreamId INT, NumRows BIGINT)
)

select @@version