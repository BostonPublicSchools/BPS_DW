DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM LongitudinalPOC.[dbo].[Lineage] WHERE TableName= 'dbo.FactStudentDiscipline')
BEGIN
    INSERT INTO LongitudinalPOC.[dbo].[Lineage]
	(
	 [TableName], 
	 [StartTime], 
	 [EndTime], 
	 [LoadType], 
	 [Status]
	)
	VALUES
	('dbo.FactStudentDiscipline', 
	 GETDATE(), 
	 NULL, 
	 'F'
	 , -- full 
	 ''
	); -- Processing
	SET @lineageKey = SCOPE_IDENTITY() ;
END;
ELSE
BEGIN
     SELECT @lineageKey = LineageKey
	 FROM LongitudinalPOC.[dbo].[Lineage]	 
	 WHERE TableName= 'dbo.FactStudentDiscipline'
END 

TRUNCATE TABLE [LongitudinalPOC].[dbo].[FactStudentDiscipline];



INSERT INTO [LongitudinalPOC].[dbo].[FactStudentDiscipline]
           ([StudentKey]
           ,[TimeKey]
           ,[SchoolKey]
           ,[DisciplineIncidentKey]           
           ,[LineageKey])

SELECT DISTINCT 
       ds.StudentKey,
       dt.TimeKey,
	   dschool.SchoolKey,	   
	   d_di.[DisciplineIncidentKey],	   
	   @lineageKey AS LineageKey
FROM  [BPSDATA-03].[ExtractAspen].[dbo].[STUDENT_CONDUCT_INCIDENT] cnd      
	  INNER JOIN LongitudinalPOC.dbo.DimStudent ds  ON CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),cnd.STD_ID_LOCAL))   = ds._sourceKey
	  INNER JOIN LongitudinalPOC.dbo.DimSchool dschool ON CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),cnd.SchoolId))  = dschool._sourceKey
	  INNER JOIN LongitudinalPOC.dbo.DimTime dt ON cnd.CND_INCIDENT_DATE = dt.SchoolDate
	                                  AND dt.SchoolKey is not null   
									  AND dschool.SchoolKey = dt.SchoolKey
	  INNER JOIN LongitudinalPOC.dbo.DimDisciplineIncident d_di ON CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),cnd.CND_INCIDENT_ID))    = d_di._sourceKey
	  
--updatng the lineage table
UPDATE LongitudinalPOC.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;

--select * from [LongitudinalPOC].[dbo].[FactStudentDiscipline]