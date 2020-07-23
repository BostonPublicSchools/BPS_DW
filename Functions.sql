--create function to derive schoolyear from a date
CREATE FUNCTION dbo.Func_GetSchoolYear
(
    @CurrentDate DATETIME
)
RETURNS INT
WITH SCHEMABINDING
AS
BEGIN

    -- Declare the return variable here
    DECLARE @Result INT;

    DECLARE @schoolYearRolloverDate DATE = '07/01/9999';


    IF (DATEPART(DAYOFYEAR, @CurrentDate) >= DATEPART(DAYOFYEAR, @schoolYearRolloverDate))
    BEGIN
        SET @Result = YEAR(@CurrentDate) + 1;
    END;
    ELSE
    BEGIN
        SET @Result = YEAR(@CurrentDate);
    END;

    -- Return the result of the function
    RETURN @Result;

END;
GO


CREATE FUNCTION dbo.Func_GetEasterHolidays
(
    @TheYear INT
)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
(
    WITH x
    AS (SELECT TheDate = DATEFROMPARTS(@TheYear, [Month], [Day])
        FROM
        (
            SELECT [Month],
                   [Day] = DaysToSunday + 28 - (31 * ([Month] / 4))
            FROM
            (
                SELECT [Month] = 3 + (DaysToSunday + 40) / 44,
                       DaysToSunday
                FROM
                (
                    SELECT DaysToSunday = paschal - ((@TheYear + (@TheYear / 4) + paschal - 13) % 7)
                    FROM
                    (
                        SELECT paschal = epact - (epact / 28)
                        FROM
                        (SELECT epact = (24 + 19 * (@TheYear % 19)) % 30) AS epact
                    ) AS paschal
                ) AS dts
            ) AS m
        ) AS d )
    SELECT TheDate,
           HolidayText = 'Easter Sunday'
    FROM x
    UNION ALL
    SELECT DATEADD(DAY, -2, TheDate),
           'Good Friday'
    FROM x
    UNION ALL
    SELECT DATEADD(DAY, 1, TheDate),
           'Easter Monday'
    FROM x
);
GO

CREATE FUNCTION [dbo].[Func_GetHolidayFromDate]
(
    @date  date
)
RETURNS NVARCHAR(200)

AS
BEGIN
    DECLARE @holidayName NVARCHAR(200) = 'Non-Holiday';
    -- for ease of typing
    DECLARE @year  int = DATEPART(YEAR, @date);
    DECLARE @month int = DATEPART(MONTH,@date);
    DECLARE @day   int = DATEPART(DAY, @date);
    DECLARE @dayName varchar(12) = DATENAME(DW, @date );

    DECLARE @nthWeekDay int = ceiling(@day / 7.0);
    DECLARE @isThursday bit = CASE WHEN @dayName LIKE 'Thursday' THEN 1 ELSE 0 END;
    DECLARE @isFriday   bit = CASE WHEN @dayName LIKE 'Friday' THEN 1 ELSE 0 END;
    DECLARE @isSaturday bit = CASE WHEN @dayName LIKE 'Saturday' THEN 1 ELSE 0 END;
    DECLARE @isSunday   bit = CASE WHEN @dayName LIKE 'Sunday' THEN 1 ELSE 0 END;
    DECLARE @isMonday   bit = CASE WHEN @dayName LIKE 'Monday' THEN 1 ELSE 0 END;
    DECLARE @isWeekend  bit = CASE WHEN @isSaturday = 1 OR @isSunday = 1 THEN 1 ELSE 0 END;
     
    ---- New Years Day
    if ((@month = 12 AND @day = 31 AND @isFriday=1)  
	OR (@month = 1 AND @day = 1 AND @isWeekend=0) 
    OR (@month = 1 AND @day = 2 AND @isMonday=1))
	  BEGIN
	     SET @holidayName = 'New Years Day';
      END
	
    ---- MLK day
    if (@month = 1 AND @isMonday = 1 AND @nthWeekDay = 3)  
	  BEGIN
	     SET @holidayName = 'MLK day';
      END

    ------ President’s Day ( 3rd Monday in February )
    if (@month = 2 AND @isMonday = 1 AND @nthWeekDay = 3) 
	  BEGIN
	     SET @holidayName = 'President’s Day';
      END

    ------ Memorial Day ( Last Monday in May )
    if (@month = 5 AND @isMonday = 1 AND DATEPART(MONTH, DATEADD(DAY, 7, @Date)) = 6)  
	  BEGIN
	     SET @holidayName = 'Memorial Day';
      END

	------ Independence Day ( July 4 )
	if ((@month = 7 AND @day = 3 AND @isFriday = 1)
	OR (@month = 7 AND @day = 4 AND @isWeekend = 0)
	OR (@month = 7 AND @day = 5 AND @isMonday = 1) )
	  BEGIN
	     SET @holidayName = 'Independence Day';
      END

    ------ Labor Day ( 1st Monday in September )
    if (@month = 9 AND @isMonday = 1 AND @nthWeekDay = 1) 
	  BEGIN
	     SET @holidayName = 'Labor Day';
      END

    ------ Columbus Day ( 2nd Monday in October )
    if (@month = 10 AND @isMonday = 1 AND @nthWeekDay = 2) 
	  BEGIN
	     SET @holidayName = 'Labor Day';
      END

    ------ Veteran’s Day ( November 11 )
	if ((@month = 11 AND @day = 10 AND @isFriday = 1)
	OR (@month = 11 AND @day = 11 AND @isWeekend = 0)
	OR (@month = 11 AND @day = 12 AND @isMonday = 1))
	  BEGIN
	     SET @holidayName = 'Veteran’s Day';
      END

    ------ Thanksgiving Day ( 4th Thursday in November )
    if (@month = 11 AND @isThursday = 1 AND @nthWeekDay = 4) 
	  BEGIN
	     SET @holidayName = 'Thanksgiving Day';
      END

    ------ Christmas Day ( December 25 )
    if ((@month = 12 AND @day = 24 AND @isFriday = 1) 
    or (@month = 12 AND @day = 25 AND @isWeekend = 0)
    or (@month = 12 AND @day = 25 AND @isMonday = 1))
	  BEGIN
	     SET @holidayName = 'Christmas Day';
      END

    RETURN @holidayName;
	
END

GO

CREATE FUNCTION dbo.Func_GetFullName
(
    @fName NVARCHAR(256),
    @mName NVARCHAR(256),
    @lName NVARCHAR(256)
)
RETURNS NVARCHAR(768)
AS
BEGIN
    DECLARE @fullName NVARCHAR(768);
    SELECT @fullName
        = LTRIM(RTRIM(LTRIM(@fName)) + RTRIM(' ' + LTRIM(ISNULL(@mName, ''))) + RTRIM(' ' + LTRIM(@lName)));
    RETURN @fullName;
END;



--SELECT [dbo].[Func_GetHolidayFromDate]('12-25-2019');
