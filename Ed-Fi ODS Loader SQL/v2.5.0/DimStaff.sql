DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM BPS_DW.[dbo].[Lineage] WHERE TableName= 'dbo.DimStaff')
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
	 FROM BPS_DW.[dbo].[Lineage]
	 WHERE TableName= 'dbo.DimStaff'
END 



INSERT INTO [dbo].[DimStaff]
           ([_sourceKey]
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
        ,s.StaffUniqueId
		,s.PersonalTitlePrefix
	    ,s.FirstName
	    ,s.MiddleName
		,LEFT(LTRIM(s.MiddleName),1) AS MiddleInitial	    
        ,s.LastSurname
		,BPS_DW.dbo.Func_GetFullName(s.FirstName,s.MiddleName,s.LastSurname) AS FullName
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
	    ,'12/31/9999' AS ValidTo
	    ,1 AS IsCurrent
	    --,@lineageKey AS [LineageKey]
FROM EdFi_BPS_Staging_Ods.edfi.Staff s 
     --sex
	 left JOIN [EdFi_BPS_Staging_Ods].edfi.SexType sex ON s.SexTypeId = sex.SexTypeId
	 left join [EdFi_BPS_Staging_Ods].edfi.OldEthnicityType oet on s.OldEthnicityTypeId = oet.OldEthnicityTypeId
	 left join [EdFi_BPS_Staging_Ods].edfi.CitizenshipStatusType cst on s.CitizenshipStatusTypeId = cst.CitizenshipStatusTypeId
	 left join [EdFi_BPS_Staging_Ods].edfi.Descriptor d_le on s.HighestCompletedLevelOfEducationDescriptorId = d_le.DescriptorId
WHERE NOT EXISTS(SELECT 1 
					FROM BPS_DW.[dbo].[DimStaff] ds 
					WHERE 'Ed-Fi|' + Convert(NVARCHAR(MAX),s.StaffUSI) = ds._sourceKey);

SELECT * FROM BPS_DW.[dbo].[DimStaff]

--updatng the lineage table
UPDATE BPS_DW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;






	






