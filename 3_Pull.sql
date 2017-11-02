-- Now we switch to a 'pull' mechanism to pull data from within SPEES
exec sp_execute_external_script @language = N'R',
@script = N'
sqlQuery <- "SELECT   CASE WHEN OBS.USAF = 999999 THEN CONCAT(''WBAN_'', OBS.WBAN) ELSE CONCAT(''USAF_'', OBS.USAF) END AS StationKey,
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
"

sqlDS <- RxSqlServerData(sqlQuery = sqlQuery
                                         , connectionString = "Server=ARVI-2016;Database=ISDLite;trusted_connection=YES"
                                         , stringsAsFactors=FALSE
                                         , rowBuffering = TRUE)

print(sqlDS@connectionString)

print (Sys.getpid())

rDF <- rxImport(sqlDS)

print(head(rDF))

print(summary(rDF))
'