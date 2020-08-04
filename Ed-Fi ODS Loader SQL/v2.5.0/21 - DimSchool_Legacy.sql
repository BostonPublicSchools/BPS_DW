DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM EdFiDW.[dbo].[Lineage] WHERE TableName= 'dbo.DimSchool')
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
	 FROM EdFiDW.[dbo].[Lineage]
	 WHERE TableName= 'dbo.DimSchool'
END 



INSERT INTO EdFiDW.[dbo].[DimSchool]
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
		   ,[SchoolCategoryType_Other_Indicator]
           ,[TitleIPartASchoolDesignationTypeCodeValue]
           ,[TitleIPartASchoolDesignation_Indicator]
		   ,OperationalStatusTypeDescriptor_CodeValue
		   ,OperationalStatusTypeDescriptor_Description		   
           ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]
           ,[LineageKey])
SELECT DISTINCT 
       'LegacyDW|' + Convert(NVARCHAR(MAX),LTRIM(RTRIM(sd.sch))) AS [_sourceKey],
        CASE WHEN ISNULL(LTRIM(RTRIM(statecd)),'N/A') IN ('','N/A') THEN 'N/A' ELSE ISNULL(LTRIM(RTRIM(statecd)),'N/A') END AS StateSchoolCode,
        CASE
		    WHEN LTRIM(RTRIM(sd.sch)) IN ('1291', '1292', '1293', '1294') THEN '1290'
			when LTRIM(RTRIM(sd.sch)) IN ('1440','1441') THEN '1440' 
			WHEN LTRIM(RTRIM(sd.sch)) IN ('4192','4192') THEN '4192' 
			WHEN LTRIM(RTRIM(sd.sch)) IN ('4031','4033') THEN '4033' 
			WHEN LTRIM(RTRIM(sd.sch)) IN ('1990','1991') THEN '1990' 
			WHEN LTRIM(RTRIM(sd.sch)) IN ('1140','4391') THEN '1140' 
			ELSE LTRIM(RTRIM(sd.sch))
		END AS UmbrellaSchoolCode,
		LTRIM(RTRIM(sd.[schname_f]))  AS ShortNameOfInstitution, 
		LTRIM(RTRIM(sd.[schname_f])) AS NameOfInstitution,
		'Combined' AS SchoolCategoryType, 
	    0  [SchoolCategoryType_Elementary_Indicator],
	    0  [SchoolCategoryType_Middle_Indicator],
	    0  [SchoolCategoryType_HighSchool_Indicator],
	    1  [SchoolCategoryType_Combined_Indicator],
		0  [SchoolCategoryType_Other_Indicator],
		'N/A' AS TitleIPartASchoolDesignationTypeCodeValue,
		0 AS TitleIPartASchoolDesignation_Indicator,
		'Inactive' AS OperationalStatusTypeDescriptor_CodeValue,	
		'Inactive' AS OperationalStatusTypeDescriptor_Description,
	    GETDATE() AS ValidFrom,
	    GETDATE() AS ValidTo,
	    0 AS IsCurrent,
	    @lineageKey AS [LineageKey]
--SELECT *
FROM EdFiDW.[Raw_LegacyDW].[SchoolData] sd
WHERE NOT EXISTS(SELECT 1 
					FROM EdFiDW.[dbo].[DimSchool] ds 
					WHERE 'Ed-Fi|' + Convert(NVARCHAR(MAX),LTRIM(RTRIM(sd.sch))) = ds._sourceKey);
						
--SELECT * FROM EdFiDW.[dbo].[DimSchool] 

--updatng the lineage table
UPDATE EdFiDW.[dbo].[Lineage]
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
FROM EdFiDW.dbo.DimSchool ds --WHERE UmbrellaSchoolCode = 1290
     INNER JOIN [EdFi_BPS_Staging_Ods].edfi.School s on ds._sourceKey = 'Ed-Fi|' + CAST(s.SchoolId AS NVARCHAR(50))
     LEFT JOIN [EdFi_BPS_Staging_Ods].edfi.EducationOrganizationIdentificationCode eoic on ds._sourceKey = 'Ed-Fi|' + CAST(eoic.EducationOrganizationId AS NVARCHAR(50)) AND eoic.EducationOrganizationIdentificationSystemDescriptorId = 433 --state 
*/

	

