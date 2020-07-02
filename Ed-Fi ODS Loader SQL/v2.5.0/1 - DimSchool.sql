DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM BPS_DW.[dbo].[Lineage] WHERE TableName= 'dbo.DimSchool')
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
	('dbo.DimSchool', 
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
	 WHERE TableName= 'dbo.DimSchool'
END 


DECLARE @EdFiSchools TABLE(
  [_sourceKey] [nvarchar](50) NOT NULL,
  [ShortNameOfInstitution] [nvarchar](500) NOT NULL,
  [NameOfInstitution] [nvarchar](500) NOT NULL,
  [SchoolCategoryType] [nvarchar](100) NOT NULL,
  GradeLevelDescriptorCodeValue [nvarchar](100) NOT NULL, 
  TitleIPartASchoolDesignationTypeCodeValue [nvarchar](500) NOT NULL ,
  OperationalStatusTypeDescriptor_CodeValue NVARCHAR(50) NOT NULL,
  OperationalStatusTypeDescriptor_Description NVARCHAR(1024) NOT NULL
)



DECLARE @DimSchool TABLE(	
	[_sourceKey] [nvarchar](50) NOT NULL,
	StateSchoolCode NVARCHAR(50) NULL,
    UmbrellaSchoolCode NVARCHAR(50) NULL,
	[ShortNameOfInstitution] [nvarchar](500) NOT NULL,
	[NameOfInstitution] [nvarchar](500) NOT NULL,
	[SchoolCategoryType] [nvarchar](100) NOT NULL,
	[SchoolCategoryType_Elementary_Indicator] [bit] NOT NULL,
	[SchoolCategoryType_Middle_Indicator] [bit] NOT NULL,
	[SchoolCategoryType_HighSchool_Indicator] [bit] NOT NULL,
	[SchoolCategoryType_Combined_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_AdultEducation_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_EarlyEducation_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_Eighthgrade_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_Eleventhgrade_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_Fifthgrade_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_Firstgrade_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_Fourthgrade_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_Grade13_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_Infanttoddler_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_Kindergarten_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_Ninthgrade_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_Other_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_Postsecondary_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_PreschoolPrekindergarten_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_Secondgrade_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_Seventhgrade_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_Sixthgrade_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_Tenthgrade_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_Thirdgrade_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_Twelfthgrade_Indicator] [bit] NOT NULL,
	[SchoolGradeLevel_Ungraded_Indicator] [bit] NOT NULL,
	[TitleIPartASchoolDesignationTypeCodeValue] [nvarchar](50) NOT NULL,
	[TitleIPartASchoolDesignation_Indicator] [bit] NOT NULL,
	OperationalStatusTypeDescriptor_CodeValue NVARCHAR(50) NOT NULL,
    OperationalStatusTypeDescriptor_Description NVARCHAR(1024) NOT NULL

 )

--retrieving all schools from the ODS
INSERT INTO @EdFiSchools ([_sourceKey] ,
						   StateSchoolCode,
						   UmbrellaSchoolCode,
						   [ShortNameOfInstitution],
						   [NameOfInstitution],
						   [SchoolCategoryType],
						   GradeLevelDescriptorCodeValue, 
						   TitleIPartASchoolDesignationTypeCodeValue,
						   OperationalStatusTypeDescriptor_CodeValue,
						   OperationalStatusTypeDescriptor_Description
						   )

SELECT 'Ed-Fi|' + Convert(NVARCHAR(MAX),s.SchoolId) AS [_sourceKey],
        eoic.IdentificationCode AS StateSchoolCode,
        CASE
		    WHEN s.SchoolId IN (1291, 1292, 1293, 1294) THEN '1290'
			when s.SchoolId IN (1440,1441) THEN '1440' 
			WHEN s.SchoolId IN (4192,4192) THEN '4192' 
			WHEN s.SchoolId IN (4031,4033) THEN '4033' 
			WHEN s.SchoolId IN (1990,1991) THEN '1990' 
			WHEN s.SchoolId IN (1140,4391) THEN '1140' 
			ELSE CAST(s.SchoolId AS NVARCHAR(50))
		END AS UmbrellaSchoolCode,
		edorg.ShortNameOfInstitution, 
		edorg.NameOfInstitution,
		sct.CodeValue AS SchoolCategoryType, 		   
		sgld.CodeValue AS GradeLevelDescriptorCodeValue,
		ISNULL(tIt.CodeValue,'N/A') AS TitleIPartASchoolDesignationTypeCodeValue,
		ISNULL(ost.CodeValue,'N/A') AS TitleIPartASchoolDesignationTypeCodeValue,	
		ISNULL(ost.[Description],'N/A') AS TitleIPartASchoolDesignationTypeCodeValue	
--SELECT distinct sct.CodeValue
FROM [EdFi_BPS_Staging_Ods].edfi.School s
INNER JOIN [EdFi_BPS_Staging_Ods].edfi.EducationOrganization edorg on s.SchoolId = edorg.EducationOrganizationId
INNER JOIN [EdFi_BPS_Staging_Ods].edfi.OperationalStatusType ost ON edorg.OperationalStatusTypeId = ost.OperationalStatusTypeId
LEFT JOIN  [EdFi_BPS_Staging_Ods].edfi.SchoolCategory sc on s.SchoolId = sc.SchoolId
LEFT JOIN  [EdFi_BPS_Staging_Ods].edfi.SchoolCategoryType sct on sc.SchoolCategoryTypeId = sct.SchoolCategoryTypeId
LEFT JOIN  [EdFi_BPS_Staging_Ods].edfi.TitleIPartASchoolDesignationType tIt on s.TitleIPartASchoolDesignationTypeId = tIt.TitleIPartASchoolDesignationTypeId
LEFT JOIN  [EdFi_BPS_Staging_Ods].edfi.SchoolGradeLevel sgl on s.SchoolId = sgl.SchoolId
LEFT JOIN  [EdFi_BPS_Staging_Ods].edfi.Descriptor sgld on sgl.GradeLevelDescriptorId = sgld.DescriptorId
LEFT JOIN  [EdFi_BPS_Staging_Ods].edfi.EducationOrganizationIdentificationCode eoic ON edorg.EducationOrganizationId = eoic.EducationOrganizationId


INSERT INTO @DimSchool ([_sourceKey]
						,StateSchoolCode
						,UmbrellaSchoolCode
						,[ShortNameOfInstitution]
						,[NameOfInstitution]
						,[SchoolCategoryType]
						,[SchoolCategoryType_Elementary_Indicator]
						,[SchoolCategoryType_Middle_Indicator]
						,[SchoolCategoryType_HighSchool_Indicator]
						,[SchoolCategoryType_Combined_Indicator]          
						,[SchoolGradeLevel_AdultEducation_Indicator]
						,[SchoolGradeLevel_EarlyEducation_Indicator]
						,[SchoolGradeLevel_Eighthgrade_Indicator]
						,[SchoolGradeLevel_Eleventhgrade_Indicator]
						,[SchoolGradeLevel_Fifthgrade_Indicator]
						,[SchoolGradeLevel_Firstgrade_Indicator]
						,[SchoolGradeLevel_Fourthgrade_Indicator]
						,[SchoolGradeLevel_Grade13_Indicator]
						,[SchoolGradeLevel_Infanttoddler_Indicator]
						,[SchoolGradeLevel_Kindergarten_Indicator]
						,[SchoolGradeLevel_Ninthgrade_Indicator]
						,[SchoolGradeLevel_Other_Indicator]
						,[SchoolGradeLevel_Postsecondary_Indicator]
						,[SchoolGradeLevel_PreschoolPrekindergarten_Indicator]
						,[SchoolGradeLevel_Secondgrade_Indicator]
						,[SchoolGradeLevel_Seventhgrade_Indicator]
						,[SchoolGradeLevel_Sixthgrade_Indicator]
						,[SchoolGradeLevel_Tenthgrade_Indicator]
						,[SchoolGradeLevel_Thirdgrade_Indicator]
						,[SchoolGradeLevel_Twelfthgrade_Indicator]
						,[SchoolGradeLevel_Ungraded_Indicator]
						,[TitleIPartASchoolDesignationTypeCodeValue]
						,[TitleIPartASchoolDesignation_Indicator],
						 OperationalStatusTypeDescriptor_CodeValue,
						 OperationalStatusTypeDescriptor_Description
						)
SELECT DISTINCT 
       [_sourceKey], 
	   StateSchoolCode,
	   UmbrellaSchoolCode,
       ShortNameOfInstitution,
       NameOfInstitution,
	   SchoolCategoryType,
	   CASE  WHEN SchoolCategoryType IN ('Elementary School') THEN 1 ELSE 0 END  [SchoolCategoryType_Elementary_Indicator],
	   CASE  WHEN SchoolCategoryType IN ('Middle School') THEN 1 ELSE 0 END  [SchoolCategoryType_Middle_Indicator],
	   CASE  WHEN SchoolCategoryType IN ('High School') THEN 1 ELSE 0 END  [SchoolCategoryType_HighSchool_Indicator],
	   CASE  WHEN SchoolCategoryType NOT IN ('Elementary School','Middle School','High School') THEN 1 ELSE 0 END  [SchoolCategoryType_Combined_Indicator],
	   
	   0 AS [SchoolGradeLevel_AdultEducation_Indicator],
	   0 AS [SchoolGradeLevel_EarlyEducationn_Indicator],
	   0 AS [SchoolGradeLevel_Eighthgrade_Indicator],
	   0 AS [SchoolGradeLevel_Eleventhgrade_Indicator],
	   0 AS [SchoolGradeLevel_Fifthgrade_Indicator],
	   0 AS [SchoolGradeLevel_Firstgrade_Indicator],
	   0 AS [SchoolGradeLevel_Fourthgrade_Indicator],
	   0 AS [SchoolGradeLevel_Grade13_Indicator],
	   0 AS [SchoolGradeLevel_Infanttoddler_Indicator],
	   0 AS [SchoolGradeLevel_Kindergarten_Indicator],
	   0 AS [SchoolGradeLevel_Ninthgrade_Indicator],
	   0 AS [SchoolGradeLevel_Other_Indicator],
	   0 AS [SchoolGradeLevel_Postsecondary_Indicator],
	   0 AS [SchoolGradeLevel_PreschoolPrekindergarten_Indicator],
	   0 AS [SchoolGradeLevel_Secondgrade_Indicator],
	   0 AS [SchoolGradeLevel_Seventhgrade_Indicator],
	   0 AS [SchoolGradeLevel_Sixthgrade_Indicator],
	   0 AS [SchoolGradeLevel_Tenthgrade_Indicator],
	   0 AS [SchoolGradeLevel_Thirdgrade_Indicator],
	   0 AS [SchoolGradeLevel_Twelfthgrade_Indicator],
	   0 AS [SchoolGradeLevel_Ungraded_Indicator],
	   TitleIPartASchoolDesignationTypeCodeValue,
	   CASE WHEN TitleIPartASchoolDesignationTypeCodeValue NOT IN ('Not designated as a Title I Part A school','N/A') THEN 1 ELSE 0 END AS TitleIPartASchoolDesignation_Indicator,
	   OperationalStatusTypeDescriptor_CodeValue,
	   OperationalStatusTypeDescriptor_Description
FROM @EdFiSchools;


--I will find a more efficient way to this. 
UPDATE ds 
SET    [SchoolGradeLevel_AdultEducation_Indicator] = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'Adult Education'

UPDATE ds 
SET    [SchoolGradeLevel_EarlyEducation_Indicator] = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'Early Education'

UPDATE ds 
SET    [SchoolGradeLevel_Eighthgrade_Indicator] = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'Eighth grade'

UPDATE ds 
SET    [SchoolGradeLevel_Eleventhgrade_Indicator] = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'Eleventh grade'

UPDATE ds 
SET    SchoolGradeLevel_Fifthgrade_Indicator = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'Fifth grade'

UPDATE ds 
SET    [SchoolGradeLevel_Firstgrade_Indicator] = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'First grade'

UPDATE ds 
SET    SchoolGradeLevel_Fourthgrade_Indicator = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'Fourth grade'

UPDATE ds 
SET    SchoolGradeLevel_Grade13_Indicator = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'Grade 13'

UPDATE ds 
SET    SchoolGradeLevel_Infanttoddler_Indicator = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'Infant/toddler'

UPDATE ds 
SET    SchoolGradeLevel_Kindergarten_Indicator = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'Kindergarten'

UPDATE ds 
SET    SchoolGradeLevel_Ninthgrade_Indicator = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'Ninth grade'

UPDATE ds 
SET    SchoolGradeLevel_Other_Indicator = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'Other'

UPDATE ds 
SET    SchoolGradeLevel_Postsecondary_Indicator = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'Postsecondary'

UPDATE ds 
SET    SchoolGradeLevel_PreschoolPrekindergarten_Indicator = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'Preschool/Prekindergarten'

UPDATE ds 
SET    SchoolGradeLevel_Secondgrade_Indicator = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'Second grade'

UPDATE ds 
SET    SchoolGradeLevel_Seventhgrade_Indicator = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'Seventh grade'

UPDATE ds 
SET    SchoolGradeLevel_Sixthgrade_Indicator = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'Sixth grade'

UPDATE ds 
SET    SchoolGradeLevel_Tenthgrade_Indicator = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'Tenth grade'


UPDATE ds 
SET    SchoolGradeLevel_Thirdgrade_Indicator = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'Third grade'


UPDATE ds 
SET    SchoolGradeLevel_Twelfthgrade_Indicator = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'Twelfth grade'


UPDATE ds 
SET    SchoolGradeLevel_Ungraded_Indicator = 1
FROM @DimSchool ds 
     INNER JOIN @EdFiSchools es ON ds._sourceKey = es._sourceKey
WHERE es.GradeLevelDescriptorCodeValue = 'Ungraded'


INSERT INTO BPS_DW.[dbo].[DimSchool]
           ([_sourceKey]
		   ,[StateSchoolCode]
		   ,[UmbrellaSchoolCode]
           ,[ShortNameOfInstitution]
           ,[NameOfInstitution]
           ,[SchoolCategoryType]
           ,[SchoolCategoryType_Elementary_Indicator]
           ,[SchoolCategoryType_Middle_Indicator]
           ,[SchoolCategoryType_HighSchool_Indicator]
           ,[SchoolCategoryType_Combined_Indicator]          
           ,[SchoolGradeLevel_AdultEducation_Indicator]
           ,[SchoolGradeLevel_EarlyEducation_Indicator]
           ,[SchoolGradeLevel_Eighthgrade_Indicator]
           ,[SchoolGradeLevel_Eleventhgrade_Indicator]
           ,[SchoolGradeLevel_Fifthgrade_Indicator]
           ,[SchoolGradeLevel_Firstgrade_Indicator]
           ,[SchoolGradeLevel_Fourthgrade_Indicator]
           ,[SchoolGradeLevel_Grade13_Indicator]
           ,[SchoolGradeLevel_Infanttoddler_Indicator]
           ,[SchoolGradeLevel_Kindergarten_Indicator]
           ,[SchoolGradeLevel_Ninthgrade_Indicator]
           ,[SchoolGradeLevel_Other_Indicator]
           ,[SchoolGradeLevel_Postsecondary_Indicator]
           ,[SchoolGradeLevel_PreschoolPrekindergarten_Indicator]
           ,[SchoolGradeLevel_Secondgrade_Indicator]
           ,[SchoolGradeLevel_Seventhgrade_Indicator]
           ,[SchoolGradeLevel_Sixthgrade_Indicator]
           ,[SchoolGradeLevel_Tenthgrade_Indicator]
           ,[SchoolGradeLevel_Thirdgrade_Indicator]
           ,[SchoolGradeLevel_Twelfthgrade_Indicator]
           ,[SchoolGradeLevel_Ungraded_Indicator]
           ,[TitleIPartASchoolDesignationTypeCodeValue]
           ,[TitleIPartASchoolDesignation_Indicator]
		   ,OperationalStatusTypeDescriptor_CodeValue
		   ,OperationalStatusTypeDescriptor_Description		   
           ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]
           ,[LineageKey])

SELECT   [_sourceKey]
        ,[StateSchoolCode]
	    ,[UmbrellaSchoolCode]
        ,[ShortNameOfInstitution]
        ,[NameOfInstitution]
        ,[SchoolCategoryType]
        ,[SchoolCategoryType_Elementary_Indicator]
        ,[SchoolCategoryType_Middle_Indicator]
        ,[SchoolCategoryType_HighSchool_Indicator]
        ,[SchoolCategoryType_Combined_Indicator]          
        ,[SchoolGradeLevel_AdultEducation_Indicator]
        ,[SchoolGradeLevel_EarlyEducation_Indicator]
        ,[SchoolGradeLevel_Eighthgrade_Indicator]
        ,[SchoolGradeLevel_Eleventhgrade_Indicator]
        ,[SchoolGradeLevel_Fifthgrade_Indicator]
        ,[SchoolGradeLevel_Firstgrade_Indicator]
        ,[SchoolGradeLevel_Fourthgrade_Indicator]
        ,[SchoolGradeLevel_Grade13_Indicator]
        ,[SchoolGradeLevel_Infanttoddler_Indicator]
        ,[SchoolGradeLevel_Kindergarten_Indicator]
        ,[SchoolGradeLevel_Ninthgrade_Indicator]
        ,[SchoolGradeLevel_Other_Indicator]
        ,[SchoolGradeLevel_Postsecondary_Indicator]
        ,[SchoolGradeLevel_PreschoolPrekindergarten_Indicator]
        ,[SchoolGradeLevel_Secondgrade_Indicator]
        ,[SchoolGradeLevel_Seventhgrade_Indicator]
        ,[SchoolGradeLevel_Sixthgrade_Indicator]
        ,[SchoolGradeLevel_Tenthgrade_Indicator]
        ,[SchoolGradeLevel_Thirdgrade_Indicator]
        ,[SchoolGradeLevel_Twelfthgrade_Indicator]
        ,[SchoolGradeLevel_Ungraded_Indicator]
        ,[TitleIPartASchoolDesignationTypeCodeValue]
        ,[TitleIPartASchoolDesignation_Indicator]
		,OperationalStatusTypeDescriptor_CodeValue
		,OperationalStatusTypeDescriptor_Description
        ,GETDATE() AS ValidFrom
	    ,CASE WHEN OperationalStatusTypeDescriptor_CodeValue IN ('Active','Added','Changed Agency','Continuing','New','Reopened') THEN '12/31/9999' ELSE GETDATE() END AS ValidTo
	    ,CASE WHEN OperationalStatusTypeDescriptor_CodeValue IN ('Active','Added','Changed Agency','Continuing','New','Reopened') THEN 1  ELSE 0  END AS IsCurrent
	    ,@lineageKey AS [LineageKey]
FROM @DimSchool s
WHERE NOT EXISTS(SELECT 1 
					FROM BPS_DW.[dbo].[DimSchool] ds 
					WHERE s._sourceKey = ds._sourceKey);

--SELECT * FROM BPS_DW.[dbo].[DimSchool]


--updatng the lineage table
UPDATE BPS_DW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;


/*

UPDATE ds 
SET  ds.StateSchoolCode = ISNULL(eoic.IdentificationCode,'N/A'),
     ds.UmbrellaSchoolCode = CASE
						WHEN s.SchoolId IN (1291, 1292, 1293, 1294) THEN '1290'
						when s.SchoolId IN (1440,1441) THEN '1440' 
						WHEN s.SchoolId IN (4192,4192) THEN '4192' 
						WHEN s.SchoolId IN (4031,4033) THEN '4033' 
						WHEN s.SchoolId IN (1990,1991) THEN '1990' 
						WHEN s.SchoolId IN (1140,4391) THEN '1140' 
						ELSE CAST(s.SchoolId AS NVARCHAR(50))
					END 
--select *
FROM BPS_DW.dbo.DimSchool ds --WHERE UmbrellaSchoolCode = 1290
     INNER JOIN [EdFi_BPS_Staging_Ods].edfi.School s on ds._sourceKey = 'Ed-Fi|' + CAST(s.SchoolId AS NVARCHAR(50))
     LEFT JOIN [EdFi_BPS_Staging_Ods].edfi.EducationOrganizationIdentificationCode eoic on ds._sourceKey = 'Ed-Fi|' + CAST(eoic.EducationOrganizationId AS NVARCHAR(50))


*/

	




