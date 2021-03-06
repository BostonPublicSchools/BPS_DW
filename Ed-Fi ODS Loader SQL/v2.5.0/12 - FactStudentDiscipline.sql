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

delete [EdFiDW].[dbo].[FactStudentDiscipline];



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
FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineIncident di       
      INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentDisciplineIncidentAssociation sdia ON di.IncidentIdentifier = sdia.IncidentIdentifier
	  INNER JOIN EdFiDW.dbo.DimSchool dschool ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),di.SchoolId)   = dschool._sourceKey
	                                          AND di.IncidentDate BETWEEN dschool.ValidFrom AND dschool.ValidTo
	  INNER JOIN EdFiDW.dbo.DimTime dt ON di.IncidentDate = dt.SchoolDate
	                                           AND dt.SchoolKey is not null   
					                 		   AND dschool.SchoolKey = dt.SchoolKey
	  INNER JOIN EdFiDW.dbo.DimStudent ds  ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),sdia.StudentUSI)   = ds._sourceKey
	                                       AND di.IncidentDate BETWEEN ds.ValidFrom AND ds.ValidTo
	  
	  INNER JOIN EdFiDW.dbo.DimDisciplineIncident d_di ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),di.IncidentIdentifier)   = d_di._sourceKey


--updatng the lineage table
UPDATE EdFiDW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;




--select * from [EdFiDW].[dbo].[FactStudentDiscipline]

/*

SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineIncident WHERE IncidentDate = '2018-08-27'
SELECT * FROM EdFiDW.dbo.DimTime WHERE SchoolDate = '2018-09-18'

SELECT MIN(IncidentDate) FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineIncident
SELECT MIN(SchoolDate) FROM  EdFiDW.dbo.DimTime
SELECT MIN(IncidentDate) FROM EdFiDW.dbo.View_StudentDiscipline 

SELECT MIN(IncidentDate) FROM EdFiDW.dbo.DimDisciplineIncident
SELECT * FROM dbo.DimStudent WHERE StudentKey = 56142
SELECT * FROM dbo.DimTime WHERE TimeKey = 56142


edfi.StudentDisciplineIncidentAssociation 

select * from [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineIncident where incidenttime = '12:48:00.0000000'  rd


--discilpline type
SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineIncidentBehavior
SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor WHERE Namespace LIKE '%behavior%'


--location
SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineIncident where incidenttime is null
SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.IncidentLocationType


--action
SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineActionDisciplineIncident
SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineActionDiscipline
SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor WHERE Namespace LIKE '%discipline%'


--reporter type
SELECT IncidentDate FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineIncident
SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor WHERE Namespace LIKE '%report%'


SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineAction



StudentDisciplineIncidentAssociation
*/
--110, 102257, 174, 07:30:00.0000000, School Threat (Threat of Destruction or Harm