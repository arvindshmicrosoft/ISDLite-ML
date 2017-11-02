-- "Pleasingly parallel" R execution demo
-- See https://blogs.msdn.microsoft.com/mlserver/2017/04/12/pleasingly-parallel-using-rxexecby/ has further details
truncate table TrainedModel
GO

-- The below script trains some 60-odd neural network models 
-- in a massively parallel execution model
exec sp_execute_external_script @language = N'R',
@script = N'
sqlQuery <- "
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
AND DATETIMEFROMPARTS(ObsYear, ObsMonth, ObsDay, ObsHour, 0, 0, 0) BETWEEN ''2007-01-01'' AND ''2017-10-29''
AND AirTemp > -500  -- outliers
"

trainingData <- RxSqlServerData(sqlQuery = sqlQuery
                                         , connectionString = "Server=.;Database=ISDLite;trusted_connection=YES"
                                         , stringsAsFactors=FALSE
                                         , rowBuffering = TRUE)

trainNNet <- function(keys, data)
{
  temperatureFormula <- AirTemp ~ ObsMonth + ObsHour + DayOfYear
  
  tryCatch(
    {
      result <- rxNeuralNet(formula = temperatureFormula,  data = data, 
                            type = "regression", acceleration = "gpu"
                            , miniBatchSize = 4096
                            , numIterations = 1
                            , netDefinition = "
                        input Data [3];
                        hidden H1 [200] tanh from Data all;
                        hidden H2 [300] tanh from H1 all;
                        hidden H3 [400] tanh from H2 all;
                        hidden H4 [500] tanh from H3 all;
                        output Out [1] linear from H4 all;  
                        ")
      
      sqlDS <- RxSqlServerData(table = "TrainedModel", connectionString = "Server=.;Database=ISDLite;trusted_connection=YES")
      
      rxWriteObject (sqlDS, keys, result, serialize = TRUE, overwrite = FALSE, compress = "gzip")
    },
    error=function(cond)
    {
      message(keys)
    }
  )
}

system.time(results <- rxExecBy(inData = trainingData, keys = c("StationKey"), func = trainNNet))
'
GO

-- Monitor the model progress (in another window)
select * 
from TrainedModel
GO
