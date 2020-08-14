DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM EdFiDW.[dbo].[Lineage] WHERE TableName= 'dbo.DimStudent')
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
	 FROM EdFiDW.[dbo].[Lineage]	 
	 WHERE TableName= 'dbo.DimStudent'
END 

SELECT DISTINCT 
       s.StudentUSI, 
	   COUNT(sr.StudentUSI) AS RaceCount,
       STRING_AGG(rt.CodeValue,',') AS RaceCodes,
	   STRING_AGG(rt.Description,',') AS RaceDescriptions,
	   CASE WHEN EXISTS (SELECT 1 
	                     FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr
						       WHERE s.StudentUSI = sr.StudentUSI
							     AND sr.RaceTypeId = 1) THEN 1
	   ELSE 
	       0	             
	   END AS Race_AmericanIndianAlaskanNative_Indicator,
	   CASE WHEN EXISTS (SELECT 1 
	                     FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr
						       WHERE s.StudentUSI = sr.StudentUSI
							     AND sr.RaceTypeId = 2) THEN 1
	   ELSE 
	       0	             
	   END AS Race_Asian_Indicator,
	   CASE WHEN EXISTS (SELECT 1 
	                     FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr
						       WHERE s.StudentUSI = sr.StudentUSI
							     AND sr.RaceTypeId = 3) THEN 1
	   ELSE 
	       0	             
	   END AS Race_BlackAfricaAmerican_Indicator,
	   CASE WHEN EXISTS (SELECT 1 
	                     FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr
						       WHERE s.StudentUSI = sr.StudentUSI
							     AND sr.RaceTypeId = 5) THEN 1
	   ELSE 
	       0	             
	   END AS Race_NativeHawaiianPacificIslander_Indicator,
	   CASE WHEN EXISTS (SELECT 1 
	                     FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr
						       WHERE s.StudentUSI = sr.StudentUSI
							     AND sr.RaceTypeId = 7) THEN 1
	   ELSE 
	       0	             
	   END AS Race_White_Indicator,
	   CASE WHEN EXISTS (SELECT 1 
	                     FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr
						       WHERE s.StudentUSI = sr.StudentUSI
							     AND sr.RaceTypeId = 4) THEN 1
	   ELSE 
	       0	             
	   END AS Race_ChooseNotRespond_Indicator,
	   CASE WHEN EXISTS (SELECT 1 
	                     FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr
						       WHERE s.StudentUSI = sr.StudentUSI
							     AND sr.RaceTypeId = 6) THEN 1
	   ELSE 
	       0	             
	   END AS Race_Other_Indicator into #StudentRaces    

FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s 
	  LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr ON s.StudentUSI = sr.StudentUSI		
	  LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.RaceType rt ON sr.RaceTypeId = rt.RaceTypeId
GROUP BY s.StudentUSI, s.HispanicLatinoEthnicity


;WITH StudentHomeRooomByYear AS
(
SELECT DISTINCT std_sa.StudentUSI, 
                std_sa.SchoolYear, 
				std_sa.SchoolId,  
				std_sa.ClassroomIdentificationCode AS HomeRoom,
				EdFiDW.dbo.Func_GetFullName(staff.FirstName,staff.MiddleName,staff.LastSurname) AS HomeRoomTeacher  
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSectionAssociation std_sa 
     INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StaffSectionAssociation staff_sa  ON std_sa.UniqueSectionCode = staff_sa.UniqueSectionCode
	                                                                                        AND std_sa.SchoolYear = staff_sa.SchoolYear
	 INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Staff staff on staff_sa.StaffUSI = staff.StaffUSI
WHERE std_sa.HomeroomIndicator = 1
     AND std_sa.SchoolYear IN (2019,2020)
)

INSERT INTO EdFiDW.[dbo].[DimStudent]
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
		   ,[StateRaceCode]
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
       CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),s.StudentUSI)) AS [_sourceKey],
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
       EdFiDW.dbo.Func_GetFullName(s.FirstName,s.MiddleName,s.LastSurname) AS FullName,
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
	   CASE WHEN sex.CodeValue  = 'Not Selected' THEN 1 ELSE 0 END AS SexType_NotSelected_Indicator, 
       
	   COALESCE(sr.RaceCodes,'N/A') AS RaceCode,	   
	   COALESCE(sr.RaceDescriptions,'N/A') AS RaceDescription,
	   CASE WHEN sr.RaceCount > 1 AND s.HispanicLatinoEthnicity = 0 THEN 'Multirace' 
	        WHEN s.HispanicLatinoEthnicity = 1 THEN 'Latinx'
		    ELSE COALESCE(sr.RaceCodes,'N/A')
	   END AS StateRaceCode,
	   sr.Race_AmericanIndianAlaskanNative_Indicator,
	   sr.Race_Asian_Indicator ,

	   sr.Race_BlackAfricaAmerican_Indicator,
	   sr.Race_NativeHawaiianPacificIslander_Indicator,
	   sr.Race_White_Indicator,
	   CASE WHEN sr.RaceCount > 1 AND s.HispanicLatinoEthnicity = 0 THEN 1 ELSE 0 END AS Race_MultiRace_Indicator, 
	   sr.Race_ChooseNotRespond_Indicator,
	   sr.Race_Other_Indicator,

	   CASE WHEN s.HispanicLatinoEthnicity = 1 THEN 'L' ELSE 'Non-L' END  AS EthnicityCode,
	   CASE WHEN s.HispanicLatinoEthnicity = 1 THEN 'Latinx' ELSE 'Non Latinx' END  AS EthnicityDescription,
	   s.HispanicLatinoEthnicity AS EthnicityHispanicLatino_Indicator,

	   CASE WHEN EXISTS (
					   SELECT 1
					   FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentProgramAssociation spa
					   WHERE CHARINDEX('Migrant', spa.ProgramName,1) > 1
							 AND spa.StudentUSI = s.StudentUSI
							 AND spa.EndDate IS NULL
				   ) THEN 1 ELSE 0 End AS Migrant_Indicator,
	   CASE WHEN EXISTS (
					   SELECT 1
					   FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentProgramAssociation spa
					   WHERE CHARINDEX('Homeless', spa.ProgramName,1) > 1
							 AND spa.StudentUSI = s.StudentUSI
							 AND spa.EndDate IS NULL
				   ) THEN 1 ELSE 0 End AS Homeless_Indicator,
        CASE WHEN EXISTS (
					   SELECT 1
					   FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSpecialEducationProgramAssociation spa
					   WHERE CHARINDEX('IEP', spa.ProgramName,1) > 1
							 AND spa.StudentUSI = s.StudentUSI
							 AND spa.IEPEndDate IS NULL
				   ) THEN 1 ELSE 0 End AS IEP_Indicator,
	   
	   COALESCE(lepd.CodeValue,'N/A') AS LimitedEnglishProficiencyDescriptor_CodeValue,
	   COALESCE(lepd.CodeValue,'N/A') AS LimitedEnglishProficiencyDescriptor_Description,
	   CASE WHEN COALESCE(lepd.CodeValue,'N/A') = 'Limited' THEN 1 ELSE 0 END AS LimitedEnglishProficiency_EnglishLearner_Indicator,
       CASE WHEN COALESCE(lepd.CodeValue,'N/A') = 'Formerly Limited' THEN 1 ELSE 0 END AS LimitedEnglishProficiency_Former_Indicator,
       CASE WHEN COALESCE(lepd.CodeValue,'N/A') = 'NotLimited' THEN 1 ELSE 0 END AS LimitedEnglishProficiency_NotEnglisLearner_Indicator,

	   COALESCE(s.EconomicDisadvantaged,0) AS EconomicDisadvantage_Indicator,
	   
	   --entry
	   ssa.EntryDate,
	   EdFiDW.dbo.Func_GetSchoolYear((ssa.EntryDate)) AS EntrySchoolYear, 
	   COALESCE(eglrt.CodeValue,'N/A') AS EntryCode,
       
	   --exit
	   ssa.ExitWithdrawDate,
	   EdFiDW.dbo.Func_GetSchoolYear((ssa.ExitWithdrawDate)) AS ExitWithdrawSchoolYear, 
	   ewt.CodeValue ExitWithdrawCode              

       ,ssa.EntryDate AS ValidFrom
	   ,case when ssa.ExitWithdrawDate is null then '12/31/9999'  else ssa.ExitWithdrawDate END AS ValidTo
	   ,case when ssa.ExitWithdrawDate is null then 1 else 0 end AS IsCurrent
	   ,@lineageKey AS [LineageKey]
--select *  
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s
    INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa ON s.StudentUSI = ssa.StudentUSI
	INNER JOIN EdFiDW.dbo.DimSchool dschool ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),ssa.SchoolId)   = dschool._sourceKey
    INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor gld  ON ssa.EntryGradeLevelDescriptorId = gld.DescriptorId
	LEFT JOIN StudentHomeRooomByYear shrby ON  s.StudentUSI = shrby.StudentUSI
	                                       AND ssa.SchoolId = shrby.SchoolId
										   AND ssa.SchoolYear = shrby.SchoolYear
    LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EntryGradeLevelReasonType eglrt ON ssa.EntryGradeLevelReasonTypeId = eglrt.EntryGradeLevelReasonTypeId
    LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.ExitWithdrawTypeDescriptor ewtd ON ssa.ExitWithdrawTypeDescriptorId = ewtd.ExitWithdrawTypeDescriptorId
    LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor ewtdd ON ewtd.ExitWithdrawTypeDescriptorId = ewtdd.DescriptorId
    LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.ExitWithdrawType ewt ON ewtd.ExitWithdrawTypeId = ewt.ExitWithdrawTypeId
	LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentElectronicMail sem ON s.StudentUSI = sem.StudentUSI
	                                                               AND sem.PrimaryEmailAddressIndicator = 1
	LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.ElectronicMailType emt ON sem.ElectronicMailTypeId = emt.ElectronicMailTypeId
    INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganization edorg ON ssa.SchoolId = edorg.EducationOrganizationId

    --lunch
	LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor food ON s.SchoolFoodServicesEligibilityDescriptorId = food.DescriptorId
    --sex
	INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SexType sex ON s.SexTypeId = sex.SexTypeId
	--state id
    LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentIdentificationCode sic ON s.StudentUSI = sic.StudentUSI
																				       AND sic.AssigningOrganizationIdentificationCode = 'State' 
    --lep
	LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor lepd ON s.LimitedEnglishProficiencyDescriptorId = lepd.DescriptorId
	
    --races
	LEFT JOIN #StudentRaces sr ON s.StudentUSI = sr.StudentUsi
	
WHERE NOT EXISTS(SELECT 1 
					FROM EdFiDW.[dbo].[DimStudent] ds 
		 		 WHERE CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),s.StudentUSI)) = ds._sourceKey)
	  AND ssa.SchoolYear IN (2019,2020);


DROP TABLE #StudentRaces; --, #StudentHomeRooomByYear;

--updatng the lineage table
UPDATE EdFiDW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;




