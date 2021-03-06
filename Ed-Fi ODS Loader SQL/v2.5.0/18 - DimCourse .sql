DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM EdFiDW.[dbo].[Lineage] WHERE TableName= 'dbo.DimCourse')
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
	('dbo.DimCourse', 
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
	 WHERE TableName= 'dbo.DimCourse'
END 

		
INSERT INTO [EdFiDW].[dbo].[DimCourse]
           ([_sourceKey]
           ,[CourseCode]
           ,[CourseTitle]
           ,[CourseDescription]
           ,[CourseLevelCharacteristicTypeDescriptor_CodeValue]
           ,[CourseLevelCharacteristicTypeDescriptor_Description]
           ,[AcademicSubjectDescriptor_CodeValue]
           ,[AcademicSubjectDescriptor_Description]
           ,[HighSchoolCourseRequirement_Indicator]
           ,[MinimumAvailableCredits]
           ,[MaximumAvailableCredits]
           ,[GPAApplicabilityType_CodeValue]
           ,[GPAApplicabilityType_Description]       
		   ,[SecondaryCourseLevelCharacteristicTypeDescriptor_CodeValue]
		   ,[SecondaryCourseLevelCharacteristicTypeDescriptor_Description]
		   ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]
           ,[LineageKey])

    
SELECT DISTINCT 
       CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),c.CourseCode)) AS [_sourceKey],
       c.CourseCode,
	   c.CourseTitle,
	   c.CourseDescription,
	   COALESCE(clct.CodeValue,'N/A') AS [CourseLevelCharacteristicTypeDescriptor_CodeValue],
	   COALESCE(clct.[Description],'N/A') AS [CourseLevelCharacteristicTypeDescriptor_Descriptor],

       COALESCE(ast.CodeValue,'N/A') AS [AcademicSubjectDescriptor_CodeValue],
	   COALESCE(ast.[Description],'N/A') AS [AcademicSubjectDescriptor_Descriptor],
	   COALESCE(c.HighSchoolCourseRequirement,0) AS [HighSchoolCourseRequirement_Indicator],

	   c.MinimumAvailableCredits,
	   c.MaximumAvailableCredits,
	   COALESCE(cgat.CodeValue,'N/A')  AS GPAApplicabilityType_CodeValue,
	   COALESCE(cgat.[Description],'N/A') AS GPAApplicabilityType_Description,
	   
	   'N/A' AS [SecondaryCourseLevelCharacteristicTypeDescriptor_CodeValue],
	   'N/A' AS [SecondaryCourseLevelCharacteristicTypeDescriptor_Description],

	   '07/01/2015' AS ValidFrom,
	   '12/31/9999' as ValidTo,
	    1 AS IsCurrent,
	   @lineageKey AS [LineageKey]
--select *
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Course c
     LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CourseLevelCharacteristic clc ON c.CourseCode = clc.CourseCode
	 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CourseLevelCharacteristicType clct ON clc.CourseLevelCharacteristicTypeId = clct.CourseLevelCharacteristicTypeId
	 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.AcademicSubjectType ast ON c.AcademicSubjectDescriptorId = ast.AcademicSubjectTypeId
	 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CourseGPAApplicabilityType cgat ON c.CourseGPAApplicabilityTypeId = cgat.CourseGPAApplicabilityTypeId
WHERE EXISTS (SELECT 1 
              FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CourseOffering co 
			  WHERE c.CourseCode = co.CourseCode
                AND co.SchoolYear IN (2019,2020))

--updatng the lineage table
UPDATE EdFiDW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;






