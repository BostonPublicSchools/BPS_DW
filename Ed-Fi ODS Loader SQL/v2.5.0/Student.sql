     SELECT
       s.StudentUniqueId,
	   --s.StudentUSI,
       sic.IdentificationCode as StateId, 
	   edorg.NameOfInstitution SchoolName,
	   ssa.EntryDate, ssa.ExitWithdrawDate, gld.CodeValue GradeLevelDescriptorCodeValue,
	   eglrt.CodeValue EntryGradeLevelReasonType,
	   ewtdd.CodeValue ExitWithdrawTypeDescriptorCodeValue, ewt.CodeValue ExitWithdrawTypeCodeValue,
	   s.FirstName, s.MiddleName, s.LastSurname,
	   s.BirthDate,
       sex.ShortDescription SexType,
	   s.EconomicDisadvantaged, 
	   s.LimitedEnglishProficiencyDescriptorId,
	   s.SchoolFoodServicesEligibilityDescriptorId,
	   (CASE 
			WHEN s.HispanicLatinoEthnicity=1 then 'Hispanic'
			ELSE 'Non Hispanic'
	    END) Ethnicity,
	  	sr1.RaceTypeId as 'American Indian - Alaskan Native', sr2.RaceTypeId as 'Asian', sr3.RaceTypeId as'Black - African American',sr4.RaceTypeId as'Choose Not to Respond',
	    sr5.RaceTypeId as'Native Hawaiian - Pacific Islander',sr6.RaceTypeId as 'Other', sr7.RaceTypeId as'White',

	    lepd.CodeValue as LimitedEnglishProficiency,
	    food.ShortDescription FreeAndReducedLunch,

	    (SELECT distinct 'Migrant' FROM edfi.StudentProgramAssociation spa WHERE spa.ProgramName like 'Migrant%' and spa.StudentUSI=s.StudentUSI and spa.EndDate is null ) Migrant,
        (SELECT distinct 'Homeless' FROM edfi.StudentProgramAssociation spa WHERE spa.ProgramName like '%Homeless%' and spa.StudentUSI=s.StudentUSI and  spa.begindate > '2019-08-14' ) Homeless,
	    (SELECT distinct 'Foster' FROM edfi.StudentProgramAssociation spa WHERE spa.ProgramName like '%Foster%' and spa.StudentUSI=s.StudentUSI and spa.EndDate is null ) foster

FROM edfi.Student s
INNER JOIN edfi.StudentSchoolAssociation ssa on s.StudentUSI = ssa.StudentUSI
INNER JOIN edfi.Descriptor gld on ssa.EntryGradeLevelDescriptorId = gld.DescriptorId
LEFT JOIN edfi.EntryGradeLevelReasonType eglrt on ssa.EntryGradeLevelReasonTypeId = eglrt.EntryGradeLevelReasonTypeId
LEFT JOIN edfi.ExitWithdrawTypeDescriptor ewtd on ssa.ExitWithdrawTypeDescriptorId = ewtd.ExitWithdrawTypeDescriptorId
LEFT JOIN edfi.Descriptor ewtdd on ewtd.ExitWithdrawTypeDescriptorId = ewtdd.DescriptorId
LEFT JOIN edfi.ExitWithdrawType ewt on ewtd.ExitWithdrawTypeId = ewt.ExitWithdrawTypeId
INNER JOIN edfi.EducationOrganization edorg on ssa.SchoolId = edorg.EducationOrganizationId
LEFT JOIN edfi.Descriptor food on s.SchoolFoodServicesEligibilityDescriptorId = food.DescriptorId
INNER JOIN edfi.SexType sex on s.SexTypeId = sex.SexTypeId
LEFT JOIN edfi.StudentIdentificationCode sic ON s.StudentUSI = sic.StudentUSI and AssigningOrganizationIdentificationCode = 'State'
LEFT JOIN edfi.Descriptor lepd on s.LimitedEnglishProficiencyDescriptorId = lepd.DescriptorId
LEFT JOIN edfi.StudentRace sr1 ON s.StudentUSI = sr1.StudentUsi and sr1.RaceTypeId = 1 --'American Indian - Alaskan Native'
LEFT JOIN edfi.StudentRace sr2 ON s.StudentUSI = sr2.StudentUsi and sr2.RaceTypeId = 2 --'Asian'
LEFT JOIN edfi.StudentRace sr3 ON s.StudentUSI = sr3.StudentUsi and sr3.RaceTypeId = 3 --'Black - African American'
LEFT JOIN edfi.StudentRace sr4 ON s.StudentUSI = sr4.StudentUsi and sr4.RaceTypeId = 4 --'Choose Not to Respond'
LEFT JOIN edfi.StudentRace sr5 ON s.StudentUSI = sr5.StudentUsi and sr5.RaceTypeId = 5 --'Native Hawaiian - Pacific Islander'
LEFT JOIN edfi.StudentRace sr6 ON s.StudentUSI = sr6.StudentUsi and sr6.RaceTypeId = 6 --'Other'
LEFT JOIN edfi.StudentRace sr7 ON s.StudentUSI = sr7.StudentUsi and sr7.RaceTypeId = 7 --'White'
order by sic.IdentificationCode;
