USE ISDLite
GO

CREATE OR ALTER procedure PredictTemp
(
	@PredDate datetime,
	@USAF bigint = 727930
)
as
begin
	DECLARE @predtempFsql float;

	-- Need to adjust for the fact that the dataset has timings in UTC
	SET @PredDate = DATEADD(hour, 7, @PredDate)

	DECLARE @predmonthsql int = DATEPART(MONTH, @PredDate);
	DECLARE @predhoursql int = DATEPART(HOUR, @PredDate);
	DECLARE @preddayofyearsql int = DATEPART(DAYOFYEAR, @PredDate);

	DECLARE @nnet varbinary(max) = (SELECT [value] 
			FROM TrainedModel
			WHERE id = @USAF);  

	EXEC sp_execute_external_script  
	@language = N'R'  
        , @script = N'
				nnet <- unserialize(trainedmodel)
				preddate <- data.frame(AirTemp = -9999, ObsMonth = predmonth, ObsHour = predhour, DayOfYear = preddayofyear)

				predicted.temp <- rxPredict(nnet, preddate)
				predtempF <- as.numeric((predicted.temp * 9 / 50) + 32)
				'
        , @params = N'@trainedmodel varbinary(max), @predmonth int, @predhour int, @preddayofyear int, @predtempF float OUTPUT'  
        , @predtempF = @predtempFsql OUTPUT
		, @predmonth = @predmonthsql
		, @predhour = @predhoursql
		, @preddayofyear = @preddayofyearsql
		, @trainedmodel = @nnet
		
		select @predDate, @predtempFsql;
end
go

-- let's try to predict some likely temperatures 
-- November
exec PredictTemp '2017-11-02 10:00'

-- January
exec PredictTemp '2017-01-02 6:00'

-- August
exec PredictTemp '2017-08-02 10:00'
