DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM BPS_DW.[dbo].[Lineage] WHERE TableName= 'dbo.FactStudentDiscipline')
BEGIN
    INSERT INTO BPS_DW.[dbo].[Lineage]
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
	 FROM BPS_DW.[dbo].[Lineage]	 
	 WHERE TableName= 'dbo.FactStudentDiscipline'
END 

TRUNCATE TABLE [BPS_DW].[dbo].[FactStudentDiscipline];


;WITH Discipline AS 
(
   SELECT DISTINCT 
       ds.StudentKey,
       dt.TimeKey,
	   dschool.SchoolKey,
	   ISNULL(di.IncidentTime,'00:00:00.0000000') AS IncidentTime,
	   d_dib.DisciplineIncidentBehaviorKey,
	   d_dil.DisciplineIncidentLocationKey,
	   d_dia.DisciplineIncidentActionKey,
	   d_dirt.DisciplineIncidentReporterTypeKey,
	   di.ReporterName,
	   ISNULL(di.ReportedToLawEnforcement,0) AS ReportedToLawEnforcement,
	   ISNULL(di.IncidentCost,0) AS IncidentCost,
	   @lineageKey AS LineageKey,
	   ROW_NUMBER() OVER (PARTITION BY ds.StudentKey, dt.TimeKey, dschool.SchoolKey,  ISNULL(di.IncidentTime,'00:00:00.0000000') ORDER BY NEWID() ) AS RowId
FROM  EdFi_BPS_Staging_Ods.edfi.DisciplineIncident di 
      INNER JOIN EdFi_BPS_Staging_Ods.edfi.StudentDisciplineIncidentAssociation sdia ON di.IncidentIdentifier = sdia.IncidentIdentifier
	  INNER JOIN EdFi_BPS_Staging_Ods.edfi.DisciplineIncidentBehavior dib ON sdia.IncidentIdentifier = dib.IncidentIdentifier
      INNER JOIN EDFi_BPS_Staging_Ods.edfi.DisciplineActionDisciplineIncident dadi ON di.IncidentIdentifier = dadi.IncidentIdentifier
	  INNER JOIN EdFi_BPS_Staging_Ods.edfi.DisciplineActionDiscipline dad ON dadi.DisciplineActionIdentifier = dad.DisciplineActionIdentifier

	  INNER JOIN BPS_DW.dbo.DimStudent ds  ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),sdia.StudentUSI)   = ds._sourceKey
	  INNER JOIN BPS_DW.dbo.DimSchool dschool ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),di.SchoolId)   = dschool._sourceKey
	  INNER JOIN BPS_DW.dbo.DimTime dt ON di.IncidentDate = dt.SchoolDate
	                                  AND dt.SchoolKey is not null   
									  AND dschool.SchoolKey = dt.SchoolKey
	  INNER JOIN BPS_DW.dbo.DimDisciplineIncidentBehavior d_dib ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),dib.BehaviorDescriptorId)   = d_dib._sourceKey
	  INNER JOIN BPS_DW.dbo.DimDisciplineIncidentLocation d_dil ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),di.IncidentLocationTypeId)   = d_dil._sourceKey
	  INNER JOIN BPS_DW.dbo.DimDisciplineIncidentAction d_dia ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),dad.DisciplineDescriptorId)   = d_dia._sourceKey
	  INNER JOIN BPS_DW.dbo.DimDisciplineIncidentReporterType d_dirt ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),di.ReporterDescriptionDescriptorId)   = d_dirt._sourceKey
  )


INSERT INTO [BPS_DW].[dbo].[FactStudentDiscipline]
           ([StudentKey]
           ,[TimeKey]
           ,[SchoolKey]
           ,[IncidentTime]
           ,[DisciplineIncidentBehaviorKey]
           ,[DisciplineIncidentLocationKey]
           ,[DisciplineIncidentActionKey]
           ,[DisciplineIncidentReporterTypeKey]
           ,[IncidentReporterName]
           ,[ReportedToLawEnforcement_Indicator]
           ,[IncidentCost]
           ,[LineageKey])

SELECT StudentKey,
       TimeKey,
	   SchoolKey,
	   IncidentTime,
	   DisciplineIncidentBehaviorKey,
	   DisciplineIncidentLocationKey,
	   DisciplineIncidentActionKey,
	   DisciplineIncidentReporterTypeKey,
	   ReporterName,
	   ReportedToLawEnforcement,
	   IncidentCost,
	   LineageKey
FROM Discipline
WHERE Discipline.RowId = 1


--updatng the lineage table
UPDATE BPS_DW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;



/*
SELECT * FROM dbo.DimStudent WHERE StudentKey = 56142
SELECT * FROM dbo.DimTime WHERE TimeKey = 56142


edfi.StudentDisciplineIncidentAssociation 

select * from EdFi_BPS_Staging_Ods.edfi.DisciplineIncident where incidenttime = '12:48:00.0000000'  rd


--discilpline type
SELECT * FROM EdFi_BPS_Staging_Ods.edfi.DisciplineIncidentBehavior
SELECT * FROM EdFi_BPS_Staging_Ods.edfi.Descriptor WHERE Namespace LIKE '%behavior%'


--location
SELECT * FROM EdFi_BPS_Staging_Ods.edfi.DisciplineIncident where incidenttime is null
SELECT * FROM EdFi_BPS_Staging_Ods.edfi.IncidentLocationType


--action
SELECT * FROM EdFi_BPS_Staging_Ods.edfi.DisciplineActionDisciplineIncident
SELECT * FROM EdFi_BPS_Staging_Ods.edfi.DisciplineActionDiscipline
SELECT * FROM EdFi_BPS_Staging_Ods.edfi.Descriptor WHERE Namespace LIKE '%discipline%'


--reporter type
SELECT IncidentDate FROM EdFi_BPS_Staging_Ods.edfi.DisciplineIncident
SELECT * FROM EdFi_BPS_Staging_Ods.edfi.Descriptor WHERE Namespace LIKE '%report%'


SELECT * FROM EdFi_BPS_Staging_Ods.edfi.DisciplineAction



StudentDisciplineIncidentAssociation
*/
