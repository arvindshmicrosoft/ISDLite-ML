USE ISDLite
GO

DROP TABLE IF EXISTS TrainedModel
GO

CREATE TABLE TrainedModel
(
	id	varchar(200) NOT NULL,
	[value] varbinary(max),
	CONSTRAINT unique_id UNIQUE (id)
)
GO

-- Train a neural network model for regression
-- and store the model into SQL
DECLARE @trainedModel varbinary(max)

exec sp_execute_external_script @language = N'R',
@script = N'
sqlQuery <- "SELECT   CASE WHEN OBS.USAF = 999999 THEN CONCAT(''WBAN_'', OBS.WBAN) ELSE CONCAT(''USAF_'', OBS.USAF) END AS StationKey,
         ObsMonth,
         ObsHour,
         DATEPART(DAYOFYEAR, DATEFROMPARTS(ObsYear, ObsMonth, ObsDay)) AS DayOfYear,
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

temperatureFormula <- AirTemp ~ ObsMonth + ObsHour + DayOfYear

sqlDS <- RxSqlServerData(sqlQuery = sqlQuery
                                         , connectionString = "Server=SOMESERVER;Database=ISDLite;trusted_connection=YES"
                                         , stringsAsFactors=FALSE
                                         , rowBuffering = TRUE)

print (Sys.getpid())

# note the change of the server name
print (sqlDS@connectionString)

model <- rxNeuralNet(formula = temperatureFormula,  data = sqlDS, 
                    type = "regression", acceleration = "gpu"
                    , miniBatchSize = 4096
					, numIterations = 100
                    , netDefinition = "
                    input Data [3];
                    hidden H1 [200] tanh from Data all;
                    hidden H2 [300] tanh from H1 all;
                    hidden H3 [400] tanh from H2 all;
                    hidden H4 [500] tanh from H3 all;
                    output Out [1] linear from H4 all;  
                    "
                     )

r_trainedModel <- serialize(model, NULL)
'
, @params = N'@r_trainedModel varbinary(max) OUTPUT'
, @r_trainedModel = @trainedModel OUTPUT

TRUNCATE TABLE TrainedModel
INSERT TrainedModel (id, [value]) VALUES (727930, @trainedModel)
GO

-- Briefly take a look at the model
SELECT * 
FROM TrainedModel
GO