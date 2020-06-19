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



INSERT INTO [BPS_DW].[dbo].[FactStudentDiscipline]
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
	  INNER JOIN BPS_DW.dbo.DimDisciplineIncident d_di ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),di.IncidentIdentifier)   = d_di._sourceKey
	  
--updatng the lineage table
UPDATE BPS_DW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;

--select * from [BPS_DW].[dbo].[FactStudentDiscipline]

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
