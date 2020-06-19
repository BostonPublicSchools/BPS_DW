DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM BPS_DW.[dbo].[Lineage] WHERE TableName= 'dbo.DimeStudent')
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
	('dbo.DimStudent', 
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
	 WHERE TableName= 'dbo.DimStudent'
END 


INSERT INTO BPS_DW.[dbo].[DimStudent]
           ([_sourceKey]
           ,[PrimaryElectronicMailAddress]
		   ,[PrimaryElectronicMailTypeDescriptor_CodeValue]
		   ,[PrimaryElectronicMailTypeDescriptor_Description]
           ,[StudentUniqueId]
           ,[StateId]
           ,[SchoolKey]
           ,[ShortNameOfInstitution]
           ,[NameOfInstitution]
           ,[GradeLevelDescriptor_CodeValue]
           ,[GradeLevelDescriptor_Description]
           ,[FirstName]
           ,[MiddleInitial]
           ,[MiddleName]
           ,[LastSurname]
           ,[FullName]
           ,[BirthDate]
           ,[StudentAge]
           ,[GraduationSchoolYear]
           ,[Homeroom]
           ,[HomeroomTeacher]
           ,[SexType_Code]
           ,[SexType_Description]
           ,[SexType_Male_Indicator]
           ,[SexType_Female_Indicator]
           ,[SexType_NotSelected_Indicator]
           ,[RaceCode]
           ,[RaceDescription]
           ,[Race_AmericanIndianAlaskanNative_Indicator]
           ,[Race_Asian_Indicator]
           ,[Race_BlackAfricaAmerican_Indicator]
           ,[Race_NativeHawaiianPacificIslander_Indicator]
           ,[Race_White_Indicator]
           ,[Race_MultiRace_Indicator]
           ,[Race_ChooseNotRespond_Indicator]
           ,[Race_Other_Indicator]
           ,[EthnicityCode]
           ,[EthnicityDescription]
           ,[EthnicityHispanicLatino_Indicator]
           ,[Migrant_Indicator]
           ,[Homeless_Indicator]
           ,[IEP_Indicator]
           ,[LimitedEnglishProficiencyDescriptor_CodeValue]
           ,[LimitedEnglishProficiencyDescriptor_Description]
           ,[LimitedEnglishProficiency_EnglishLearner_Indicator]
           ,[LimitedEnglishProficiency_Former_Indicator]
           ,[LimitedEnglishProficiency_NotEnglisLearner_Indicator]
           ,[EconomicDisadvantage_Indicator]
           ,[EntryDate]
           ,[EntrySchoolYear]
           ,[EntryCode]
           ,[ExitWithdrawDate]
           ,[ExitWithdrawSchoolYear]
           ,[ExitWithdrawCode]
           ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]
           ,[LineageKey])

SELECT 
       'Ed-Fi|' + Convert(NVARCHAR(MAX),s.StudentUSI) AS [_sourceKey],
	   sem.ElectronicMailAddress AS [PrimaryElectronicMailAddress],
	   emt.CodeValue AS [PrimaryElectronicMailTypeDescriptor_CodeValue],
	   emt.Description AS [PrimaryElectronicMailTypeDescriptor_Description],
	   s.StudentUniqueId,       
       sic.IdentificationCode AS StateId,
       
	   dschool.SchoolKey,
	   edorg.ShortNameOfInstitution,
	   edorg.NameOfInstitution,
	   gld.CodeValue GradeLevelDescriptor_CodeValue,
	   gld.Description AS GradeLevelDescriptor_Description,

	   s.FirstName,
	   LEFT(LTRIM(s.MiddleName),1) AS MiddleInitial,
	   s.MiddleName,	   
       s.LastSurname,
       BPS_DW.dbo.Func_GetFullName(s.FirstName,s.MiddleName,s.LastSurname) AS FullName,
	   s.BirthDate,
       DATEDIFF(YEAR, s.BirthDate, GetDate()) AS StudentAge,
	   ssa.GraduationSchoolYear,

	   NULL AS Homeroom,
	   NULL AS HomeroomTeacher,

	   CASE 
	        WHEN sex.CodeValue  = 'Male' THEN 'M'
	        WHEN sex.CodeValue  = 'Female' THEN 'F'
		    ELSE 'NS' -- not selected
	   END AS SexType_Code,
	   sex.Description AS SexType_Description,
	   CASE WHEN sex.CodeValue  = 'Male' THEN 1 ELSE 0 END AS SexType_Male_Indicator,
	   CASE WHEN sex.CodeValue  = 'Female' THEN 1 ELSE 0 END AS SexType_Female_Indicator,
	   CASE WHEN sex.CodeValue  = 'Not Selected' THEN 1 ELSE 0 END AS SexType_NotSelected_Indicator, -- NON BINARY

	   ISNULL(rt.CodeValue,'N/A') AS RaceCode,
	   ISNULL(rt.Description,'N/A') AS RaceDescription,
	   CASE WHEN  sr.StudentUsi IS NOT NULL  AND sr.RaceTypeId =1 THEN 1 ELSE 0 END AS Race_AmericanIndianAlaskanNative_Indicator,
	   CASE WHEN  sr.StudentUsi IS NOT NULL  AND sr.RaceTypeId =2 THEN 1 ELSE 0 END AS Race_Asian_Indicator,
	   CASE WHEN  sr.StudentUsi IS NOT NULL  AND sr.RaceTypeId =3 THEN 1 ELSE 0 END AS Race_BlackAfricaAmerican_Indicator,
	   CASE WHEN  sr.StudentUsi IS NOT NULL  AND sr.RaceTypeId =5 THEN 1 ELSE 0 END AS Race_NativeHawaiianPacificIslander_Indicator,
	   CASE WHEN  sr.StudentUsi IS NOT NULL  AND sr.RaceTypeId =7 THEN 1 ELSE 0 END AS Race_White_Indicator,
	   0 AS Race_MultiRace_Indicator, -- did not see this in populated template
	   CASE WHEN  sr.StudentUsi IS NOT NULL  AND sr.RaceTypeId =4 THEN 1 ELSE 0 END AS Race_ChooseNotRespond_Indicator,
	   CASE WHEN  sr.StudentUsi IS NOT NULL  AND sr.RaceTypeId =6 THEN 1 ELSE 0 END AS Race_Other_Indicator,

	   CASE WHEN s.HispanicLatinoEthnicity = 1 THEN 'H' ELSE 'Non-H' END  AS EthnicityCode,
	   CASE WHEN s.HispanicLatinoEthnicity = 1 THEN 'Hispanic' ELSE 'Non Hispanic' END  AS EthnicityDescription,
	   s.HispanicLatinoEthnicity AS EthnicityHispanicLatino_Indicator,

	   CASE WHEN EXISTS (
					   SELECT 1
					   FROM [EdFi_BPS_Staging_Ods].edfi.StudentProgramAssociation spa
					   WHERE CHARINDEX('Migrant', spa.ProgramName,1) > 1
							 AND spa.StudentUSI = s.StudentUSI
							 AND spa.EndDate IS NULL
				   ) THEN 1 ELSE 0 End AS Migrant_Indicator,
	   CASE WHEN EXISTS (
					   SELECT 1
					   FROM [EdFi_BPS_Staging_Ods].edfi.StudentProgramAssociation spa
					   WHERE CHARINDEX('Homeless', spa.ProgramName,1) > 1
							 AND spa.StudentUSI = s.StudentUSI
							 AND spa.EndDate IS NULL
				   ) THEN 1 ELSE 0 End AS Homeless_Indicator,
        CASE WHEN EXISTS (
					   SELECT *
					   FROM [EdFi_BPS_Staging_Ods].edfi.StudentSpecialEducationProgramAssociation spa
					   WHERE CHARINDEX('IEP', spa.ProgramName,1) > 1 -- Will it have a name?
							 AND spa.StudentUSI = s.StudentUSI
							 AND spa.IEPEndDate IS NULL
				   ) THEN 1 ELSE 0 End AS IEP_Indicator,
	   
	   ISNULL(lepd.CodeValue,'N/A') AS LimitedEnglishProficiencyDescriptor_CodeValue,
	   ISNULL(lepd.CodeValue,'N/A') AS LimitedEnglishProficiencyDescriptor_Description,
	   CASE WHEN ISNULL(lepd.CodeValue,'N/A') = 'L' THEN 1 ELSE 0 END AS LimitedEnglishProficiency_EnglishLearner_Indicator,
       CASE WHEN ISNULL(lepd.CodeValue,'N/A') = 'F' THEN 1 ELSE 0 END AS LimitedEnglishProficiency_Former_Indicator,
       CASE WHEN ISNULL(lepd.CodeValue,'N/A') = 'N' THEN 1 ELSE 0 END AS LimitedEnglishProficiency_NotEnglisLearner_Indicator,


	   ISNULL(s.EconomicDisadvantaged,0) AS EconomicDisadvantage_Indicator,
       
	   --entry
	   ssa.EntryDate,
	   BPS_DW.dbo.Func_GetSchoolYear((ssa.EntryDate)) AS EntrySchoolYear, 
	   ISNULL(eglrt.CodeValue,'N/A') AS EntryCode,
       
	   --exit
	   ssa.ExitWithdrawDate,
	   BPS_DW.dbo.Func_GetSchoolYear((ssa.ExitWithdrawDate)) AS ExitWithdrawSchoolYear, 
	   ewt.CodeValue ExitWithdrawCode              

       ,GETDATE() AS ValidFrom
	   ,case when ssa.ExitWithdrawDate is null then '12/31/9999'  else ssa.ExitWithdrawDate END AS ValidTo
	   ,case when ssa.ExitWithdrawDate is null then 1 else 0 end AS IsCurrent
	   ,@lineageKey AS [LineageKey]
--select *  
FROM [EdFi_BPS_Staging_Ods].edfi.Student s
    INNER JOIN [EdFi_BPS_Staging_Ods].edfi.StudentSchoolAssociation ssa ON s.StudentUSI = ssa.StudentUSI
	INNER JOIN BPS_DW.dbo.DimSchool dschool ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),ssa.SchoolId)   = dschool._sourceKey
    INNER JOIN [EdFi_BPS_Staging_Ods].edfi.Descriptor gld  ON ssa.EntryGradeLevelDescriptorId = gld.DescriptorId
    LEFT JOIN [EdFi_BPS_Staging_Ods].edfi.EntryGradeLevelReasonType eglrt ON ssa.EntryGradeLevelReasonTypeId = eglrt.EntryGradeLevelReasonTypeId
    LEFT JOIN [EdFi_BPS_Staging_Ods].edfi.ExitWithdrawTypeDescriptor ewtd ON ssa.ExitWithdrawTypeDescriptorId = ewtd.ExitWithdrawTypeDescriptorId
    LEFT JOIN [EdFi_BPS_Staging_Ods].edfi.Descriptor ewtdd ON ewtd.ExitWithdrawTypeDescriptorId = ewtdd.DescriptorId
    LEFT JOIN [EdFi_BPS_Staging_Ods].edfi.ExitWithdrawType ewt ON ewtd.ExitWithdrawTypeId = ewt.ExitWithdrawTypeId
	LEFT JOIN [EdFi_BPS_Staging_Ods].edfi.StudentElectronicMail sem ON s.StudentUSI = sem.StudentUSI
	                                                               AND sem.PrimaryEmailAddressIndicator = 1
	LEFT JOIN [EdFi_BPS_Staging_Ods].edfi.ElectronicMailType emt ON sem.ElectronicMailTypeId = emt.ElectronicMailTypeId
    INNER JOIN [EdFi_BPS_Staging_Ods].edfi.EducationOrganization edorg ON ssa.SchoolId = edorg.EducationOrganizationId

    --lunch
	LEFT JOIN [EdFi_BPS_Staging_Ods].edfi.Descriptor food ON s.SchoolFoodServicesEligibilityDescriptorId = food.DescriptorId
    --sex
	INNER JOIN [EdFi_BPS_Staging_Ods].edfi.SexType sex ON s.SexTypeId = sex.SexTypeId
	--state id
    LEFT JOIN [EdFi_BPS_Staging_Ods].edfi.StudentIdentificationCode sic ON s.StudentUSI = sic.StudentUSI
																				       AND sic.AssigningOrganizationIdentificationCode = 'State' 
    --lep
	LEFT JOIN [EdFi_BPS_Staging_Ods].edfi.Descriptor lepd ON s.LimitedEnglishProficiencyDescriptorId = lepd.DescriptorId
	
    --races
	LEFT JOIN [EdFi_BPS_Staging_Ods].edfi.StudentRace sr ON s.StudentUSI = sr.StudentUsi
	LEFT JOIN [EdFi_BPS_Staging_Ods].edfi.RaceType rt ON sr.RaceTypeId = rt.RaceTypeId	
WHERE NOT EXISTS(SELECT 1 
					FROM BPS_DW.[dbo].[DimStudent] ds 
					WHERE 'Ed-Fi|' + Convert(NVARCHAR(MAX),s.StudentUSI) = ds._sourceKey)
	  AND ssa.SchoolYear IN (2019,2020)
ORDER BY sic.IdentificationCode;

--updatng the lineage table
UPDATE BPS_DW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;


--SELECT * FROM  BPS_DW.dbo.DimStudent


/*

SELECT *
FROM [EdFi_BPS_Staging_Ods].edfi.Descriptor
WHERE Namespace LIKE '%datatype%';

SELECT *
FROM [EdFi_BPS_Staging_Ods].edfi.ResultDatatypeType;

SELECT *
FROM [EdFi_BPS_Staging_Ods].edfi.SexType;

SELECT *
FROM [EdFi_BPS_Staging_Ods].edfi.StudentRace;

SELECT *
FROM [EdFi_BPS_Staging_Ods].edfi.Program;

SELECT *
FROM [EdFi_BPS_Staging_Ods].edfi.EntryGradeLevelReasonType;

SELECT *
FROM [EdFi_BPS_Staging_Ods].edfi.StudentSchoolAssociation;


SELECT *
FROM [EdFi_BPS_Staging_Ods].edfi.RaceType;

select * from [EdFi_BPS_Staging_Ods].edfi.StudentElectronicMail


*/