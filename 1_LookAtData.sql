USE ISDLite
GO

-- Let us start by looking at the weather data for Seattle-Tacoma airport
SELECT   CASE WHEN OBS.USAF = 999999 THEN CONCAT('WBAN_', OBS.WBAN) ELSE CONCAT('USAF_', OBS.USAF) END AS StationKey,
         ObsMonth,
         ObsHour,
         DATEPART(dy, DATEFROMPARTS(ObsYear, ObsMonth, ObsDay)) AS DayOfYear,
         DATEFROMPARTS(ObsYear, ObsMonth, ObsDay) AS FullDate,
         AirTemp as Celsius,
		 (AirTemp * 9 / 5 + 32) as Fahrenheit
FROM     Observations AS OBS WITH (NOLOCK)
         INNER JOIN
         [isd-history] AS ISD WITH (NOLOCK)
         ON OBS.USAF = ISD.USAF
            AND OBS.WBAN = ISD.WBAN
WHERE    ISD.State = 'WA'
         AND ISD.CTRY = 'US'
         AND ISD.USAF = 727930
         AND DATETIMEFROMPARTS(ObsYear, ObsMonth, ObsDay, ObsHour, 0, 0, 0) BETWEEN '2007-01-01' AND '2017-10-29'
         AND AirTemp > -500  -- remove outliers
ORDER BY DATETIMEFROMPARTS(ObsYear, ObsMonth, ObsDay, ObsHour, 0, 0, 0);
GO