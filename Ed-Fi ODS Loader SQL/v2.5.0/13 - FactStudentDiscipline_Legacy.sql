DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM EdFiDW.[dbo].[Lineage] WHERE TableName= 'dbo.FactStudentDiscipline')
BEGIN
    INSERT INTO EdFiDW.[dbo].[Lineage]
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
	 FROM EdFiDW.[dbo].[Lineage]	 
	 WHERE TableName= 'dbo.FactStudentDiscipline'
END 

--TRUNCATE TABLE [EdFiDW].[dbo].[FactStudentDiscipline];

INSERT INTO [EdFiDW].[dbo].[FactStudentDiscipline]
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
FROM  [EdFiDW].[Raw_LegacyDW].[DisciplineIncidents] di    
	  INNER JOIN EdFiDW.dbo.DimStudent ds  ON CONCAT_WS('|', 'LegacyDW', Convert(NVARCHAR(MAX),di.BPS_Student_ID))   = ds._sourceKey
	                                     AND	 di.CND_INCIDENT_DATE BETWEEN ds.ValidFrom AND ds.ValidTo
	  INNER JOIN EdFiDW.dbo.DimSchool dschool ON CONCAT_WS('|', 'Ed-Fi', Convert(NVARCHAR(MAX),di.[SKL_SCHOOL_ID]))   = dschool._sourceKey 
                                         AND	 di.CND_INCIDENT_DATE BETWEEN dschool.ValidFrom AND dschool.ValidTo
	  INNER JOIN EdFiDW.dbo.DimTime dt ON di.CND_INCIDENT_DATE = dt.SchoolDate
	                                  AND dt.SchoolKey is not null   
									  AND dschool.SchoolKey = dt.SchoolKey
	  INNER JOIN EdFiDW.dbo.DimDisciplineIncident d_di ON CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),di.CND_INCIDENT_ID))    = d_di._sourceKey
WHERE TRY_CAST(di.CND_INCIDENT_DATE AS DATETIME)  > '2015-09-01'

--updatng the lineage table
UPDATE EdFiDW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;

--select * from [EdFiDW].[dbo].[FactStudentDiscipline]