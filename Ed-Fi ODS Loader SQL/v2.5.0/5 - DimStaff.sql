DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM LongitudinalPOC.[dbo].[Lineage] WHERE TableName= 'dbo.DimStaff')
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
	('dbo.DimStaff', 
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
	 WHERE TableName= 'dbo.DimStaff'
END 



INSERT INTO LongitudinalPOC.[dbo].[DimStaff]
           ([_sourceKey]
		   ,[PrimaryElectronicMailAddress]
		   ,[PrimaryElectronicMailTypeDescriptor_CodeValue]
		   ,[PrimaryElectronicMailTypeDescriptor_Description]
           ,[StaffUniqueId]
           ,[PersonalTitlePrefix]
           ,[FirstName]
           ,[MiddleName]
           ,[MiddleInitial]
           ,[LastSurname]
           ,[FullName]
           ,[GenerationCodeSuffix]
           ,[MaidenName]
           ,[BirthDate]
           ,[StaffAge]
           ,[SexType_Code]
           ,[SexType_Description]
           ,[SexType_Male_Indicator]
           ,[SexType_Female_Indicator]
           ,[SexType_NotSelected_Indicator]
           ,[HispanicLatinoEthnicity_Indicator]
           ,[OldEthnicityType_CodeValue]
           ,[OldEthnicityType_Description]
           ,[CitizenshipStatusType_CodeValue]
           ,[CitizenshipStatusType_Description]
           ,[HighestLevelOfEducationDescriptorDescriptor_CodeValue]
           ,[HighestLevelOfEducationDescriptorDescriptor_Description]
           ,[YearsOfPriorProfessionalExperience]
           ,[YearsOfPriorTeachingExperience]
           ,[HighlyQualifiedTeacher_Indicator]

           ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]
           ,[LineageKey])
    

select  distinct 
         'Ed-Fi|' + Convert(NVARCHAR(MAX),s.StaffUSI) AS [_sourceKey]
		,sem.ElectronicMailAddress AS [PrimaryElectronicMailAddress]
	    ,emt.CodeValue AS [PrimaryElectronicMailTypeDescriptor_CodeValue]
	    ,emt.Description AS [PrimaryElectronicMailTypeDescriptor_Description]
        ,s.StaffUniqueId
		,s.PersonalTitlePrefix
	    ,s.FirstName
	    ,s.MiddleName
		,LEFT(LTRIM(s.MiddleName),1) AS MiddleInitial	    
        ,s.LastSurname
		,LongitudinalPOC.dbo.Func_GetFullName(s.FirstName,s.MiddleName,s.LastSurname) AS FullName
		,s.GenerationCodeSuffix
		,s.MaidenName        
		,s.BirthDate
        ,DATEDIFF(YEAR, s.BirthDate, GetDate()) AS [StaffAge]
		,CASE 
	        WHEN sex.CodeValue  = 'Male' THEN 'M'
	        WHEN sex.CodeValue  = 'Female' THEN 'F'
		    ELSE 'NS' -- not selected
	    END AS SexType_Code
	    ,ISNULL(sex.CodeValue,'Not Selected') AS SexType_Description
	    ,CASE WHEN ISNULL(sex.CodeValue,'Not Selected')  = 'Male' THEN 1 ELSE 0 END AS SexType_Male_Indicator
	    ,CASE WHEN ISNULL(sex.CodeValue,'Not Selected')  = 'Female' THEN 1 ELSE 0 END AS SexType_Female_Indicator
	    ,CASE WHEN ISNULL(sex.CodeValue,'Not Selected')  = 'Not Selected' THEN 1 ELSE 0 END AS SexType_NotSelected_Indicator
		,s.HispanicLatinoEthnicity as [HispanicLatinoEthnicity_Indicator]
		,ISNULL(oet.CodeValue,'N/A') as [OldEthnicityType_CodeValue]
		,ISNULL(oet.Description,'N/A') as [OldEthnicityType_Description]
		,ISNULL(cst.CodeValue,'N/A') as [CitizenshipStatusType_CodeValue]
		,ISNULL(cst.Description,'N/A') as [CitizenshipStatusType_Description]
		,ISNULL(d_le.CodeValue,'N/A') as [HighestLevelOfEducationDescriptorDescriptor_CodeValue]
		,ISNULL(d_le.Description,'N/A') as [HighestLevelOfEducationDescriptorDescriptor_Description]
		,s.YearsOfPriorProfessionalExperience
		,s.YearsOfPriorTeachingExperience
		,s.HighlyQualifiedTeacher
        ,GETDATE() AS ValidFrom
	    ,case when seoea.EndDate IS null then  '12/31/9999' else seoea.EndDate  END AS ValidTo
	    ,case when seoea.EndDate IS NULL THEN  1 else 0 end AS IsCurrent
	    ,@lineageKey AS [LineageKey]
FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Staff s 
     INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StaffSchoolAssociation ssa ON s.StaffUSI = ssa.StaffUSI
	 INNER JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StaffEducationOrganizationEmploymentAssociation seoea ON s.StaffUSI = seoea.StaffUSI
	 --sex	 
	 left JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SexType sex ON s.SexTypeId = sex.SexTypeId
	 left join [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.OldEthnicityType oet on s.OldEthnicityTypeId = oet.OldEthnicityTypeId
	 left join [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CitizenshipStatusType cst on s.CitizenshipStatusTypeId = cst.CitizenshipStatusTypeId
	 left join [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d_le on s.HighestCompletedLevelOfEducationDescriptorId = d_le.DescriptorId
	 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StaffElectronicMail sem ON s.StaffUSI = sem.StaffUSI
	                                                              AND sem.PrimaryEmailAddressIndicator = 1
	 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.ElectronicMailType emt ON sem.ElectronicMailTypeId = emt.ElectronicMailTypeId
WHERE NOT EXISTS(SELECT 1 
					FROM LongitudinalPOC.[dbo].[DimStaff] ds 
					WHERE 'Ed-Fi|' + Convert(NVARCHAR(MAX),s.StaffUSI) = ds._sourceKey)
	  AND ssa.SchoolYear IN (2019,2020);



--updatng the lineage table
UPDATE LongitudinalPOC.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;

/*
SELECT * FROM EdFi_BPS_Staging_Ods.edfi.Staff
SELECT * FROM EdFi_BPS_Staging_Ods.edfi.StaffSchoolAssociation
SELECT * FROM EdFi_BPS_Staging_Ods.edfi.StaffEducationOrganizationEmploymentAssociation

*/









	






