
/*
---------------------------------------------------------------------------------------------------------------------
--******************************* INDEXING **************************************************************************
---------------------------------------------------------------------------------------------------------------------

--slowly changing dimensions -  four-part non-clustered index - covering index
CREATE NONCLUSTERED INDEX DimSchool_CoveringIndex
  ON EdFiDW.dbo.DimSchool (_sourceKey, ValidFrom)
INCLUDE ( ValidTo, SchoolKey);

CREATE NONCLUSTERED INDEX DimStudent_CoveringIndex
  ON EdFiDW.dbo.DimStudent (_sourceKey, ValidFrom)
INCLUDE ( ValidTo, StudentKey);

CREATE NONCLUSTERED INDEX DimAttendanceEventCategory_CoveringIndex
  ON EdFiDW.dbo.DimAttendanceEventCategory(_sourceKey, ValidFrom)
INCLUDE ( ValidTo, AttendanceEventCategoryKey);

CREATE NONCLUSTERED INDEX DimAssessment_CoveringIndex
  ON EdFiDW.dbo.DimAssessment(_sourceKey, ValidFrom)
INCLUDE ( ValidTo, AssessmentKey);

CREATE NONCLUSTERED INDEX DimCourse_CoveringIndex
  ON EdFiDW.dbo.DimCourse(_sourceKey, ValidFrom)
INCLUDE ( ValidTo, CourseKey);


--Facts Tables - Using ColumnStore Indexes
CREATE COLUMNSTORE INDEX CSI_FactStudentAttendanceByDay
  ON EdFiDW.dbo.FactStudentAttendanceByDay
  ([StudentKey]
  ,[TimeKey]
  ,[SchoolKey]
  ,[AttendanceEventCategoryKey]
  ,[AttendanceEventReason]
  ,[LineageKey])

CREATE COLUMNSTORE INDEX CSI_FactStudentDiscipline
  ON EdFiDW.dbo.FactStudentDiscipline
  ([StudentKey]
  ,[TimeKey]
  ,[SchoolKey]
  ,[DisciplineIncidentKey]
  ,[LineageKey])

CREATE COLUMNSTORE INDEX CSI_FactStudentAssessmentScore
  ON EdFiDW.dbo.FactStudentAssessmentScore
  ([StudentKey]
  ,[TimeKey]
  ,[AssessmentKey]
  ,[ScoreResult]
  ,[IntegerScoreResult]
  ,[DecimalScoreResult]
  ,[LiteralScoreResult]
  ,[LineageKey])

CREATE COLUMNSTORE INDEX CSI_FactStudentCourseTranscript
  ON EdFiDW.dbo.FactStudentCourseTranscript
  ([StudentKey]
  ,[TimeKey]
  ,[CourseKey]
  ,[SchoolKey]
  ,[EarnedCredits]
  ,[PossibleCredits]
  ,[FinalLetterGradeEarned]
  ,[FinalNumericGradeEarned]
  ,[LineageKey])

DROP INDEX CSI_FactStudentAttendanceByDay
  ON EdFiDW.dbo.FactStudentAttendanceByDay

  */

---------------------------------------------------------------------------------------------------------------------
--******************************* Starting Data Q/A *****************************************************************
---------------------------------------------------------------------------------------------------------------------
SELECT 'Starting data quality process.............'
UNION ALL


--DimSchool
------------------------------------------------------------------------------------------
SELECT ' Entity being analyzed: DimSchool'
UNION ALL
----Total
SELECT CONCAT('     Analyzing: Total. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (SELECT COUNT(DISTINCT _sourceKey)
				 FROM EdFiDW.[dbo].[DimSchool] 
				 WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0),
			   '  ',
			   'Records in source system:',
			   (SELECT COUNT(DISTINCT eo.EducationOrganizationId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganization eo
				WHERE EXISTS(SELECT 1 
							 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School s 
							 WHERE s.SchoolId = eo.EducationOrganizationId))
	        )
UNION ALL
----Total Active
SELECT CONCAT('     Analyzing: Total Active. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (SELECT COUNT(DISTINCT SchoolKey)
				 FROM EdFiDW.[dbo].[DimSchool] 
				 WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				 AND IsCurrent = 1),
			   '  ',
			   'Records in source system:',
			   (SELECT COUNT(DISTINCT eo.EducationOrganizationId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganization eo
				WHERE EXISTS(SELECT 1 
							 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School s 
							 WHERE s.SchoolId = eo.EducationOrganizationId)
				  AND EXISTS(SELECT 1 
							 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.OperationalStatusType ost
							 WHERE eo.OperationalStatusTypeId = ost.OperationalStatusTypeId 
							   AND COALESCE(ost.CodeValue,'N/A') IN ('Active','Added','Changed Agency','Continuing','New','Reopened'))) 
	         )
UNION ALL
----Total Elementary
SELECT CONCAT('     Analyzing: Total Elementary. => Source: Ed-Fi ODS => ',
			  'Records in entity:',
			  (SELECT COUNT(DISTINCT SchoolKey)
			  FROM EdFiDW.[dbo].[DimSchool] 
			  WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
			  AND SchoolCategoryType  = 'Elementary School'),
			  '  ',
			  'Records in source system:',
			  (SELECT COUNT(DISTINCT eo.EducationOrganizationId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganization eo
				WHERE EXISTS(SELECT 1 
			 				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School s 
			 				WHERE s.SchoolId = eo.EducationOrganizationId)
				AND EXISTS(SELECT 1 
			 				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolCategory sc 
								INNER JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolCategoryType sct on sc.SchoolCategoryTypeId = sct.SchoolCategoryTypeId
							WHERE  eo.EducationOrganizationId = sc.SchoolId
							AND sct.CodeValue  IN ('Elementary School') )) 
		      )
UNION ALL
----Total Middle
SELECT CONCAT('     Analyzing: Total Middle. => Source: Ed-Fi ODS => ',
			  'Records in entity:',
			  (SELECT COUNT(DISTINCT SchoolKey)
			  FROM EdFiDW.[dbo].[DimSchool] 
			  WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
			  AND SchoolCategoryType  = 'Middle School'),
			  '  ',
			  'Records in source system:',
			  (SELECT COUNT(DISTINCT eo.EducationOrganizationId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganization eo
				WHERE EXISTS(SELECT 1 
			 				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School s 
			 				WHERE s.SchoolId = eo.EducationOrganizationId)
				AND EXISTS(SELECT 1 
			 				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolCategory sc 
								INNER JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolCategoryType sct on sc.SchoolCategoryTypeId = sct.SchoolCategoryTypeId
							WHERE  eo.EducationOrganizationId = sc.SchoolId
							AND sct.CodeValue  IN ('Middle School') )) 
		      )
UNION ALL
----Total High
SELECT CONCAT('     Analyzing: Total High. => Source: Ed-Fi ODS => ',
			  'Records in entity:',
			  (SELECT COUNT(DISTINCT SchoolKey)
			  FROM EdFiDW.[dbo].[DimSchool] 
			  WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
			  AND SchoolCategoryType  = 'High School'),
			  '  ',
			  'Records in source system:',
			  (SELECT COUNT(DISTINCT eo.EducationOrganizationId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganization eo
				WHERE EXISTS(SELECT 1 
			 				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School s 
			 				WHERE s.SchoolId = eo.EducationOrganizationId)
				AND EXISTS(SELECT 1 
			 				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolCategory sc 
								INNER JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolCategoryType sct on sc.SchoolCategoryTypeId = sct.SchoolCategoryTypeId
							WHERE  eo.EducationOrganizationId = sc.SchoolId
							AND sct.CodeValue  IN ('High School') )) 
		      )
UNION ALL
----Total Combined
SELECT CONCAT('     Analyzing: Total Combined. => Source: Ed-Fi ODS => ',
			  'Records in entity:',
			  (SELECT COUNT(DISTINCT SchoolKey)
			  FROM EdFiDW.[dbo].[DimSchool] 
			  WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
			  AND SchoolCategoryType  NOT IN ('Elementary School','Middle School','High School') ),
			  '  ',
			  'Records in source system:',
			  (SELECT COUNT(DISTINCT eo.EducationOrganizationId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganization eo
				WHERE EXISTS(SELECT 1 
			 				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School s 
			 				WHERE s.SchoolId = eo.EducationOrganizationId)
				AND EXISTS(SELECT 1 
			 				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolCategory sc 
								INNER JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolCategoryType sct on sc.SchoolCategoryTypeId = sct.SchoolCategoryTypeId
							WHERE  eo.EducationOrganizationId = sc.SchoolId
							AND sct.CodeValue  NOT IN ('Elementary School','Middle School','High School')  )) 
		      )
UNION ALL
----Total Title 1
SELECT CONCAT('     Analyzing: Total Title I. => Source: Ed-Fi ODS => ',
			  'Records in entity:',
			  (
			   SELECT COUNT(DISTINCT SchoolKey)
			   FROM EdFiDW.[dbo].[DimSchool] 
			   WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
			     AND TitleIPartASchoolDesignationTypeCodeValue  NOT IN ('Not designated as a Title I Part A school','N/A') 
			  ),
			  '  ',
			  'Records in source system:',
			  (
			   SELECT COUNT(DISTINCT s.SchoolId) 
			   FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School s
			   WHERE  EXISTS(SELECT 1 
			 				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.TitleIPartASchoolDesignationType t1 								
							WHERE  s.TitleIPartASchoolDesignationTypeId = t1.TitleIPartASchoolDesignationTypeId
							AND t1.CodeValue NOT IN ('Not designated as a Title I Part A school','N/A') )
			  ) 
		    )
					   

--DimStudent
------------------------------------------------------------------------------------------
UNION ALL
SELECT ' --------------------------------------------------------------------------------------------------------------------------------------'
UNION ALL
SELECT ' Entity being analyzed: DimStudent'
UNION ALL
----Total
SELECT CONCAT('     Analyzing: Total. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (SELECT COUNT(DISTINCT StudentUniqueId)
				 FROM EdFiDW.[dbo].[DimStudent] 
				 WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0),
			   '  ',
			   'Records in source system:',
			   (SELECT COUNT(DISTINCT s.StudentUniqueId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s				     
				WHERE EXISTS(SELECT 1 
							 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
							 WHERE s.StudentUSI = ssa.StudentUSI
							   AND ssa.SchoolYear IN (2019,2020)))
	        )
UNION ALL
----Total Active
SELECT CONCAT('     Analyzing: Total Active. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (SELECT COUNT(DISTINCT StudentUniqueId)
				 FROM EdFiDW.[dbo].[DimStudent] 
				 WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				   AND IsCurrent = 1),
			   '  ',
			   'Records in source system:',
			   (SELECT COUNT(DISTINCT s.StudentUniqueId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s				     
				WHERE EXISTS(SELECT 1 
							 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
							 WHERE s.StudentUSI = ssa.StudentUSI
							   AND ssa.SchoolYear IN (2019,2020)
							   AND ssa.ExitWithdrawDate IS NULL))
	        )
UNION ALL
----Total Race AmericanIndianAlaskanNative
SELECT CONCAT('     Analyzing: Total Race AmericanIndianAlaskanNative. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (SELECT COUNT(DISTINCT StudentUniqueId)
				 FROM EdFiDW.[dbo].[DimStudent] 
				 WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				   AND Race_AmericanIndianAlaskanNative_Indicator = 1),
			   '  ',
			   'Records in source system:',
			   (SELECT COUNT(DISTINCT s.StudentUniqueId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s				     
				WHERE EXISTS(SELECT 1 
							 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
							 WHERE s.StudentUSI = ssa.StudentUSI
							   AND ssa.SchoolYear IN (2019,2020))
				 AND  EXISTS(SELECT 1 
				 			 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr 
				 			 WHERE s.StudentUSI = sr.StudentUSI
				 			   AND sr.RaceTypeId = 1))
	        )
UNION ALL
----Total Race Asian
SELECT CONCAT('     Analyzing: Total Race Asian. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (SELECT COUNT(DISTINCT StudentUniqueId)
				 FROM EdFiDW.[dbo].[DimStudent] 
				 WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				   AND Race_Asian_Indicator = 1),
			   '  ',
			   'Records in source system:',
			   (SELECT COUNT(DISTINCT s.StudentUniqueId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s				     
				WHERE EXISTS(SELECT 1 
							 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
							 WHERE s.StudentUSI = ssa.StudentUSI
							   AND ssa.SchoolYear IN (2019,2020))
				 AND  EXISTS(SELECT 1 
				 			 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr 
				 			 WHERE s.StudentUSI = sr.StudentUSI
				 			   AND sr.RaceTypeId = 2))
	        )
UNION ALL
----Total Race BlackAfricaAmerican
SELECT CONCAT('     Analyzing: Total Race BlackAfricaAmerican. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (SELECT COUNT(DISTINCT StudentUniqueId)
				 FROM EdFiDW.[dbo].[DimStudent] 
				 WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				   AND Race_BlackAfricaAmerican_Indicator = 1),
			   '  ',
			   'Records in source system:',
			   (SELECT COUNT(DISTINCT s.StudentUniqueId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s				     
				WHERE EXISTS(SELECT 1 
							 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
							 WHERE s.StudentUSI = ssa.StudentUSI
							   AND ssa.SchoolYear IN (2019,2020))
				 AND  EXISTS(SELECT 1 
				 			 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr 
				 			 WHERE s.StudentUSI = sr.StudentUSI
				 			   AND sr.RaceTypeId = 3))
	        )
UNION ALL
----Total Race NativeHawaiianPacificIslander
SELECT CONCAT('     Analyzing: Total Race NativeHawaiianPacificIslander. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (SELECT COUNT(DISTINCT StudentUniqueId)
				 FROM EdFiDW.[dbo].[DimStudent] 
				 WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				   AND Race_NativeHawaiianPacificIslander_Indicator = 1),
			   '  ',
			   'Records in source system:',
			   (SELECT COUNT(DISTINCT s.StudentUniqueId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s				     
				WHERE EXISTS(SELECT 1 
							 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
							 WHERE s.StudentUSI = ssa.StudentUSI
							   AND ssa.SchoolYear IN (2019,2020))
				 AND  EXISTS(SELECT 1 
				 			 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr 
				 			 WHERE s.StudentUSI = sr.StudentUSI
				 			   AND sr.RaceTypeId = 5))
	        )
UNION ALL
----Total Race White
SELECT CONCAT('     Analyzing: Total Race White. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (SELECT COUNT(DISTINCT StudentUniqueId)
				 FROM EdFiDW.[dbo].[DimStudent] 
				 WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				   AND Race_White_Indicator = 1),
			   '  ',
			   'Records in source system:',
			   (SELECT COUNT(DISTINCT s.StudentUniqueId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s				     
				WHERE EXISTS(SELECT 1 
							 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
							 WHERE s.StudentUSI = ssa.StudentUSI
							   AND ssa.SchoolYear IN (2019,2020))
				 AND  EXISTS(SELECT 1 
				 			 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr 
				 			 WHERE s.StudentUSI = sr.StudentUSI
				 			   AND sr.RaceTypeId = 7))
	        )
UNION ALL
----Total Race ChooseNotRespond
SELECT CONCAT('     Analyzing: Total Race ChooseNotRespond. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (SELECT COUNT(DISTINCT StudentUniqueId)
				 FROM EdFiDW.[dbo].[DimStudent] 
				 WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				   AND Race_ChooseNotRespond_Indicator = 1),
			   '  ',
			   'Records in source system:',
			   (SELECT COUNT(DISTINCT s.StudentUniqueId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s				     
				WHERE EXISTS(SELECT 1 
							 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
							 WHERE s.StudentUSI = ssa.StudentUSI
							   AND ssa.SchoolYear IN (2019,2020))
				 AND  EXISTS(SELECT 1 
				 			 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr 
				 			 WHERE s.StudentUSI = sr.StudentUSI
				 			   AND sr.RaceTypeId =4))
	        )
UNION ALL
----Total Race Other
SELECT CONCAT('     Analyzing: Total Race Other. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (SELECT COUNT(DISTINCT StudentUniqueId)
				 FROM EdFiDW.[dbo].[DimStudent] 
				 WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				   AND Race_Other_Indicator = 1),
			   '  ',
			   'Records in source system:',
			   (SELECT COUNT(DISTINCT s.StudentUniqueId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s				     
				WHERE EXISTS(SELECT 1 
							 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
							 WHERE s.StudentUSI = ssa.StudentUSI
							   AND ssa.SchoolYear IN (2019,2020))
				 AND  EXISTS(SELECT 1 
				 			 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr 
				 			 WHERE s.StudentUSI = sr.StudentUSI
				 			   AND sr.RaceTypeId = 6))
	        )
UNION ALL
----Total Race Multirace
SELECT CONCAT('     Analyzing: Total Race Multirace. => Source: Ed-Fi ODS => ',
				   'Records in entity:',
				   (SELECT COUNT(DISTINCT StudentUniqueId)
					 FROM EdFiDW.[dbo].[DimStudent] 
					 WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
					   AND Race_MultiRace_Indicator = 1),
				   '  ',
				   'Records in source system:',
				   (
					SELECT COUNT(DISTINCT s.StudentUniqueId) 
					FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s				     
					WHERE EXISTS(SELECT 1 
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
								 WHERE s.StudentUSI = ssa.StudentUSI
								   AND ssa.SchoolYear IN (2019,2020))
						 AND  (SELECT COUNT(sr.StudentUSI) 
				 				 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr 
				 				 WHERE s.StudentUSI = sr.StudentUSI)   > 1 
						 AND s.HispanicLatinoEthnicity = 0				 
				  )
		      )
UNION ALL
----Total Ethnicity Hispanic
SELECT CONCAT('     Analyzing: Total Ethnicity Hispanic. => Source: Ed-Fi ODS => ',
				   'Records in entity:',
				   (SELECT COUNT(DISTINCT StudentUniqueId)
					 FROM EdFiDW.[dbo].[DimStudent] 
					 WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
					   AND EthnicityHispanicLatino_Indicator = 1),
				   '  ',
				   'Records in source system:',
				   (
					SELECT COUNT(DISTINCT s.StudentUniqueId) 
					FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s				     
					WHERE EXISTS(SELECT 1 
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
								 WHERE s.StudentUSI = ssa.StudentUSI
								   AND ssa.SchoolYear IN (2019,2020))						 
						 AND s.HispanicLatinoEthnicity = 1				 
				  )
		      )
UNION ALL
----Total Migrant
SELECT CONCAT('     Analyzing: Total Migrant. => Source: Ed-Fi ODS => ',
				   'Records in entity:',
				   (SELECT COUNT(DISTINCT StudentUniqueId)
					FROM EdFiDW.[dbo].[DimStudent] 
					WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
					   AND Migrant_Indicator = 1),
				   '  ',
				   'Records in source system:',
				   (
					SELECT COUNT(DISTINCT s.StudentUniqueId) 
					FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s				     
					WHERE EXISTS(SELECT 1 
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
								 WHERE s.StudentUSI = ssa.StudentUSI
								   AND ssa.SchoolYear IN (2019,2020))
					 AND  EXISTS(SELECT 1
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentProgramAssociation spa
								 WHERE CHARINDEX('Migrant', spa.ProgramName,1) > 1
										AND spa.StudentUSI = s.StudentUSI
										AND spa.EndDate IS NULL)				 
				   )
		      )
UNION ALL
----Total Homeless
SELECT CONCAT('     Analyzing: Total Homeless. => Source: Ed-Fi ODS => ',
				   'Records in entity:',
				   (SELECT COUNT(DISTINCT StudentUniqueId)
					FROM EdFiDW.[dbo].[DimStudent] 
					WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
					   AND Homeless_Indicator = 1),
				   '  ',
				   'Records in source system:',
				   (
					SELECT COUNT(DISTINCT s.StudentUniqueId) 
					FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s				     
					WHERE EXISTS(SELECT 1 
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
								 WHERE s.StudentUSI = ssa.StudentUSI
								   AND ssa.SchoolYear IN (2019,2020))
					 AND  EXISTS(SELECT 1
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentProgramAssociation spa
								 WHERE CHARINDEX('Homeless', spa.ProgramName,1) > 1
										AND spa.StudentUSI = s.StudentUSI
										AND spa.EndDate IS NULL)				 
				   )
		      )
UNION ALL
----Total IEP
SELECT CONCAT('     Analyzing: Total IEP. => Source: Ed-Fi ODS => ',
				   'Records in entity:',
				   (SELECT COUNT(DISTINCT StudentUniqueId)
					FROM EdFiDW.[dbo].[DimStudent] 
					WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
					   AND IEP_Indicator = 1),
				   '  ',
				   'Records in source system:',
				   (
					SELECT COUNT(DISTINCT s.StudentUniqueId) 
					FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s				     
					WHERE EXISTS(SELECT 1 
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
								 WHERE s.StudentUSI = ssa.StudentUSI
								   AND ssa.SchoolYear IN (2019,2020))
					 AND  EXISTS(SELECT 1
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentProgramAssociation spa
								 WHERE CHARINDEX('IEP', spa.ProgramName,1) > 1
										AND spa.StudentUSI = s.StudentUSI
										AND spa.EndDate IS NULL)				 
				   )
		      )
UNION ALL
----Total LEP English Learner
SELECT CONCAT('     Analyzing: Total LEP English Learner. => Source: Ed-Fi ODS => ',
				   'Records in entity:',
				   (SELECT COUNT(DISTINCT StudentUniqueId)
					FROM EdFiDW.[dbo].[DimStudent] 
					WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
					   AND LimitedEnglishProficiency_EnglishLearner_Indicator = 1),
				   '  ',
				   'Records in source system:',
				   (
					SELECT COUNT(DISTINCT s.StudentUniqueId) 
					FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s				     
					WHERE EXISTS(SELECT 1 
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
								 WHERE s.StudentUSI = ssa.StudentUSI
								   AND ssa.SchoolYear IN (2019,2020))
					 AND  EXISTS(SELECT 1
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d
								 WHERE d.DescriptorId = s.LimitedEnglishProficiencyDescriptorId
								   AND COALESCE(d.CodeValue,'N/A') = 'Limited')				 
				   )
		      )
UNION ALL
----Total LEP Former
SELECT CONCAT('     Analyzing: Total LEP Former. => Source: Ed-Fi ODS => ',
				   'Records in entity:',
				   (SELECT COUNT(DISTINCT StudentUniqueId)
					FROM EdFiDW.[dbo].[DimStudent] 
					WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
					   AND LimitedEnglishProficiency_Former_Indicator = 1),
				   '  ',
				   'Records in source system:',
				   (
					SELECT COUNT(DISTINCT s.StudentUniqueId) 
					FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s				     
					WHERE EXISTS(SELECT 1 
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
								 WHERE s.StudentUSI = ssa.StudentUSI
								   AND ssa.SchoolYear IN (2019,2020))
					 AND  EXISTS(SELECT 1
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d
								 WHERE d.DescriptorId = s.LimitedEnglishProficiencyDescriptorId
								   AND COALESCE(d.CodeValue,'N/A') = 'Formerly Limited')				 
				   )
		      )
UNION ALL
----Total LEP Not EnglisLearner
SELECT CONCAT('     Analyzing: Total LEP Not EnglisLearner. => Source: Ed-Fi ODS => ',
				   'Records in entity:',
				   (SELECT COUNT(DISTINCT StudentUniqueId)
					FROM EdFiDW.[dbo].[DimStudent] 
					WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
					   AND LimitedEnglishProficiency_NotEnglisLearner_Indicator = 1),
				   '  ',
				   'Records in source system:',
				   (
					SELECT COUNT(DISTINCT s.StudentUniqueId) 
					FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s				     
					WHERE EXISTS(SELECT 1 
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
								 WHERE s.StudentUSI = ssa.StudentUSI
								   AND ssa.SchoolYear IN (2019,2020))
					 AND  EXISTS(SELECT 1
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d
								 WHERE d.DescriptorId = s.LimitedEnglishProficiencyDescriptorId
								   AND COALESCE(d.CodeValue,'N/A') = 'NotLimited')				 
				   )
		      )
UNION ALL
----Total Economically Disadvantaged
SELECT CONCAT('     Analyzing: Total Economically Disadvantaged. => Source: Ed-Fi ODS => ',
				   'Records in entity:',
				   (SELECT COUNT(DISTINCT StudentUniqueId)
					FROM EdFiDW.[dbo].[DimStudent] 
					WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
					   AND EconomicDisadvantage_Indicator = 1),
				   '  ',
				   'Records in source system:',
				   (
					SELECT COUNT(DISTINCT s.StudentUniqueId) 
					FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s				     
					WHERE EXISTS(SELECT 1 
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
								 WHERE s.StudentUSI = ssa.StudentUSI
								   AND ssa.SchoolYear IN (2019,2020))
					 AND  s.EconomicDisadvantaged = 1 
				   )
		      )

--DimAttendanceEventCategory
------------------------------------------------------------------------------------------
UNION ALL
SELECT ' --------------------------------------------------------------------------------------------------------------------------------------'
UNION ALL
SELECT ' Entity being analyzed: DimAttendanceEventCategory'
UNION ALL
----Total
SELECT CONCAT('     Analyzing: Total Attendance Codes. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (
			    SELECT COUNT(DISTINCT _sourceKey)
				FROM EdFiDW.[dbo].[DimAttendanceEventCategory] 
				WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
			   ),
			   '  ',
			   'Records in source system:',
			   (
			    SELECT COUNT(DISTINCT d.DescriptorId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d			     
				WHERE d.Namespace IN ('http://ed-fi.org/Descriptor/AttendanceEventCategoryDescriptor.xml',
				                      'http://ed-fi.org/Descriptor/Follett/Aspen/AttendanceEventCategoryDescriptor.xml')
			   )
	        )
UNION ALL
----Total Active
SELECT CONCAT('     Analyzing: Total Active. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (
			    SELECT COUNT(DISTINCT _sourceKey)
				FROM EdFiDW.[dbo].[DimAttendanceEventCategory] 
				WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				  AND IsCurrent = 1
			   ),
			   '  ',
			   'Records in source system:',
			   (
			    SELECT COUNT(DISTINCT d.DescriptorId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d			     
				WHERE d.Namespace IN ('http://ed-fi.org/Descriptor/AttendanceEventCategoryDescriptor.xml',
				                      'http://ed-fi.org/Descriptor/Follett/Aspen/AttendanceEventCategoryDescriptor.xml')
			   )
	        )
UNION ALL
----Total In-Attendance
SELECT CONCAT('     Analyzing: Total In-Attendance Category. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (
			    SELECT COUNT(DISTINCT _sourceKey)
				FROM EdFiDW.[dbo].[DimAttendanceEventCategory] 
				WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				  AND InAttendance_Indicator = 1
			   ),
			   '  ',
			   'Records in source system:',
			   (
			    SELECT COUNT(DISTINCT d.DescriptorId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d			     
				WHERE d.Namespace IN ('http://ed-fi.org/Descriptor/AttendanceEventCategoryDescriptor.xml',
				                      'http://ed-fi.org/Descriptor/Follett/Aspen/AttendanceEventCategoryDescriptor.xml')
				  AND COALESCE(d.CodeValue,'In Attendance') in ('In Attendance','Tardy','Early departure')
			   )
	        )
UNION ALL
----Total Unexcused Category
SELECT CONCAT('     Analyzing: Total Unexcused Category. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (
			    SELECT COUNT(DISTINCT _sourceKey)
				FROM EdFiDW.[dbo].[DimAttendanceEventCategory] 
				WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				  AND UnexcusedAbsence_Indicator = 1
			   ),
			   '  ',
			   'Records in source system:',
			   (
			    SELECT COUNT(DISTINCT d.DescriptorId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d			     
				WHERE d.Namespace IN ('http://ed-fi.org/Descriptor/AttendanceEventCategoryDescriptor.xml',
				                      'http://ed-fi.org/Descriptor/Follett/Aspen/AttendanceEventCategoryDescriptor.xml')
				  AND COALESCE(d.CodeValue,'In Attendance') in ('Unexcused Absence')
			   )
	        )
UNION ALL
----Total Excused Category
SELECT CONCAT('     Analyzing: Total Excused Category. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (
			    SELECT COUNT(DISTINCT _sourceKey)
				FROM EdFiDW.[dbo].[DimAttendanceEventCategory] 
				WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				  AND ExcusedAbsence_Indicator = 1
			   ),
			   '  ',
			   'Records in source system:',
			   (
			    SELECT COUNT(DISTINCT d.DescriptorId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d			     
				WHERE d.Namespace IN ('http://ed-fi.org/Descriptor/AttendanceEventCategoryDescriptor.xml',
				                      'http://ed-fi.org/Descriptor/Follett/Aspen/AttendanceEventCategoryDescriptor.xml')
				  AND COALESCE(d.CodeValue,'In Attendance') in ('Excused Absence')
			   )
	        )
UNION ALL
----Total Tardy Category
SELECT CONCAT('     Analyzing: Total Tardy Category. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (
			    SELECT COUNT(DISTINCT _sourceKey)
				FROM EdFiDW.[dbo].[DimAttendanceEventCategory] 
				WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				  AND Tardy_Indicator = 1
			   ),
			   '  ',
			   'Records in source system:',
			   (
			    SELECT COUNT(DISTINCT d.DescriptorId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d			     
				WHERE d.Namespace IN ('http://ed-fi.org/Descriptor/AttendanceEventCategoryDescriptor.xml',
				                      'http://ed-fi.org/Descriptor/Follett/Aspen/AttendanceEventCategoryDescriptor.xml')
				  AND COALESCE(d.CodeValue,'In Attendance') in ('Tardy')
			   )
	        )
UNION ALL
----Total Early departure Category
SELECT CONCAT('     Analyzing: Total Early departure Category. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (
			    SELECT COUNT(DISTINCT _sourceKey)
				FROM EdFiDW.[dbo].[DimAttendanceEventCategory] 
				WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				  AND EarlyDeparture_Indicator = 1
			   ),
			   '  ',
			   'Records in source system:',
			   (
			    SELECT COUNT(DISTINCT d.DescriptorId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d			     
				WHERE d.Namespace IN ('http://ed-fi.org/Descriptor/AttendanceEventCategoryDescriptor.xml',
				                      'http://ed-fi.org/Descriptor/Follett/Aspen/AttendanceEventCategoryDescriptor.xml')
				  AND COALESCE(d.CodeValue,'In Attendance') in ('Early departure')
			   )
	        )
--DimDisciplineIncident
------------------------------------------------------------------------------------------
UNION ALL
SELECT ' --------------------------------------------------------------------------------------------------------------------------------------'
UNION ALL
SELECT ' Entity being analyzed: DimDisciplineIncident'
UNION ALL
----Total
SELECT CONCAT('     Analyzing: Total Incidents. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (
			    SELECT COUNT(DISTINCT _sourceKey)
				FROM EdFiDW.[dbo].[DimDisciplineIncident] 
				WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
			   ),
			   '  ',
			   'Records in source system:', 
			   (
			    SELECT COUNT(DISTINCT di.IncidentIdentifier) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineIncident di			     
				  INNER JOIN EdFiDW.dbo.DimTime dt ON di.IncidentDate = dt.SchoolDate
				  INNER JOIN EdFiDW.dbo.DimSchool dschool ON  CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),di.SchoolId))   = dschool._sourceKey
				                                          AND dt.SchoolKey is not null   
				                                          AND dschool.SchoolKey = dt.SchoolKey
				WHERE EdFiDW.dbo.Func_GetSchoolYear(di.IncidentDate) IN (2019,2020)
			   )
	        )
UNION ALL
----Total With Actions
SELECT CONCAT('     Analyzing: Total Incidents With Action(s). => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (
			    SELECT COUNT(DISTINCT _sourceKey)
				FROM EdFiDW.[dbo].[DimDisciplineIncident] 
				WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				AND [DisciplineDescriptor_CodeValue] <> 'N/A'
			   ),
			   '  ',
			   'Records in source system:',
			   (
			    SELECT COUNT(DISTINCT di.IncidentIdentifier) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineIncident di
				     INNER JOIN EdFiDW.dbo.DimTime dt ON di.IncidentDate = dt.SchoolDate
				     INNER JOIN EdFiDW.dbo.DimSchool dschool ON  CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),di.SchoolId))   = dschool._sourceKey
				                                             AND dt.SchoolKey is not null   
				                                             AND dschool.SchoolKey = dt.SchoolKey				
				     INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineActionDisciplineIncident dadi ON di.IncidentIdentifier = dadi.IncidentIdentifier
            	     INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineActionDiscipline dad ON dadi.DisciplineActionIdentifier = dad.DisciplineActionIdentifier
				WHERE EdFiDW.dbo.Func_GetSchoolYear(di.IncidentDate) IN (2019,2020)
			   )
	        )
UNION ALL
----Total Incidents That Resulted in ISS
SELECT CONCAT('     Analyzing: Total Incidents That Resulted in ISS. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (
			    SELECT COUNT(DISTINCT _sourceKey)
				FROM EdFiDW.[dbo].[DimDisciplineIncident] 
				WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				AND DisciplineDescriptor_ISS_Indicator = 1
			   ),
			   '  ',
			   'Records in source system:',
			   (
			    SELECT COUNT(DISTINCT di.IncidentIdentifier) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineIncident di
				     INNER JOIN EdFiDW.dbo.DimTime dt ON di.IncidentDate = dt.SchoolDate
				     INNER JOIN EdFiDW.dbo.DimSchool dschool ON  CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),di.SchoolId))   = dschool._sourceKey
				                                             AND dt.SchoolKey is not null   
				                                             AND dschool.SchoolKey = dt.SchoolKey				
				     INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineActionDisciplineIncident dadi ON di.IncidentIdentifier = dadi.IncidentIdentifier
            	     INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineActionDiscipline dad ON dadi.DisciplineActionIdentifier = dad.DisciplineActionIdentifier
					 INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d_dia ON dad.DisciplineDescriptorId   = d_dia.DescriptorId
				WHERE EdFiDW.dbo.Func_GetSchoolYear(di.IncidentDate) IN (2019,2020)
				  AND COALESCE(d_dia.CodeValue,'N/A') IN ('In School Suspension','In-School Suspension')
			   )
	        )
UNION ALL
----Total Incidents That Resulted in OSS
SELECT CONCAT('     Analyzing: Total Incidents That Resulted in OSS. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (
			    SELECT COUNT(DISTINCT _sourceKey)
				FROM EdFiDW.[dbo].[DimDisciplineIncident] 
				WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				AND DisciplineDescriptor_OSS_Indicator = 1
			   ),
			   '  ',
			   'Records in source system:',
			   (
			    SELECT COUNT(DISTINCT di.IncidentIdentifier) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineIncident di
				     INNER JOIN EdFiDW.dbo.DimTime dt ON di.IncidentDate = dt.SchoolDate
				     INNER JOIN EdFiDW.dbo.DimSchool dschool ON  CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),di.SchoolId))   = dschool._sourceKey
				                                             AND dt.SchoolKey is not null   
				                                             AND dschool.SchoolKey = dt.SchoolKey				
				     INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineActionDisciplineIncident dadi ON di.IncidentIdentifier = dadi.IncidentIdentifier
            	     INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineActionDiscipline dad ON dadi.DisciplineActionIdentifier = dad.DisciplineActionIdentifier
					 INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d_dia ON dad.DisciplineDescriptorId   = d_dia.DescriptorId
				WHERE EdFiDW.dbo.Func_GetSchoolYear(di.IncidentDate) IN (2019,2020)
				  AND COALESCE(d_dia.CodeValue,'N/A') IN ('Out of School Suspension','Out-Of-School Suspension')
			   )
	        )
--DimAssessment
------------------------------------------------------------------------------------------
UNION ALL
SELECT ' --------------------------------------------------------------------------------------------------------------------------------------'
UNION ALL
SELECT ' Entity being analyzed: DimAssessment'
UNION ALL
----Total
SELECT CONCAT('     Analyzing: Total. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (
			    SELECT COUNT(DISTINCT AssessmentIdentifier)
				FROM EdFiDW.[dbo].[DimAssessment] 
				WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
			   ),
			   '  ',
			   'Records in source system:', 
			   (
			    SELECT COUNT(DISTINCT a.AssessmentIdentifier) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Assessment	a		     
				WHERE CHARINDEX('MCAS',a.AssessmentIdentifier,1) = 1 
			   )
	        )
UNION ALL
----Total Assessment Score Types
SELECT CONCAT('     Analyzing: Total Assessment Score Types. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (
			    SELECT COUNT(DISTINCT ReportingMethodDescriptor_CodeValue)
				FROM EdFiDW.[dbo].[DimAssessment] 
				WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				  AND AssessmentScore_Indicator = 1
			   ),
			   '  ',
			   'Records in source system:', 
			   (
			    SELECT COUNT(DISTINCT a_s_armt.CodeValue) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Assessment	a		     
				     INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.AssessmentScore a_s ON a.AssessmentIdentifier = a_s.AssessmentIdentifier 
					 INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.AssessmentReportingMethodType a_s_armt ON a_s.AssessmentReportingMethodTypeId = a_s_armt.AssessmentReportingMethodTypeId
				WHERE CHARINDEX('MCAS',a.AssessmentIdentifier,1) = 1 
			   )
	        )
UNION ALL
----Total Assessment Performance Level Types
SELECT CONCAT('     Analyzing: Total Performance Level Types. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (
			    SELECT COUNT(DISTINCT ReportingMethodDescriptor_CodeValue)
				FROM EdFiDW.[dbo].[DimAssessment] 
				WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				  AND AssessmentPerformanceLevel_Indicator = 1
			   ),
			   '  ',
			   'Records in source system:', 
			   (
			    SELECT COUNT(DISTINCT a_pl_armt.CodeValue) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Assessment	a		     
				     INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.AssessmentPerformanceLevel a_pl ON a.AssessmentIdentifier = a_pl.AssessmentIdentifier 
					 INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.AssessmentReportingMethodType a_pl_armt ON a_pl.AssessmentReportingMethodTypeId = a_pl_armt.AssessmentReportingMethodTypeId
				WHERE CHARINDEX('MCAS',a.AssessmentIdentifier,1) = 1 
			   )
	        )			
--DimCourse
------------------------------------------------------------------------------------------
UNION ALL
SELECT ' --------------------------------------------------------------------------------------------------------------------------------------'
UNION ALL
SELECT ' Entity being analyzed: DimCourse'
UNION ALL
----Total
SELECT CONCAT('     Analyzing: Total. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (
			    SELECT COUNT(DISTINCT _sourceKey)
				FROM EdFiDW.[dbo].[DimCourse] 
				WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
			   ),
			   '  ',
			   'Records in source system:', 
			   (
			    SELECT COUNT(DISTINCT c.CourseCode) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Course c	     
				WHERE EXISTS (SELECT 1 
							  FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CourseOffering co 
							  WHERE c.CourseCode = co.CourseCode
							  AND co.SchoolYear IN (2019,2020))
			   )
	        )
UNION ALL
----Total Course Types
SELECT CONCAT('     Analyzing: Total Course Types. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (
			    SELECT COUNT(DISTINCT CourseLevelCharacteristicTypeDescriptor_CodeValue)
				FROM EdFiDW.[dbo].[DimCourse] 
				WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				  AND CourseLevelCharacteristicTypeDescriptor_CodeValue <> 'N/A'
			   ),
			   '  ',
			   'Records in source system:', 
			   (
			    SELECT COUNT(DISTINCT clct.CodeValue) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Course c	 
				     INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CourseLevelCharacteristic clc ON c.CourseCode = clc.CourseCode
	                 INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CourseLevelCharacteristicType clct ON clc.CourseLevelCharacteristicTypeId = clct.CourseLevelCharacteristicTypeId
				WHERE EXISTS (SELECT 1 
							  FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CourseOffering co 
							  WHERE c.CourseCode = co.CourseCode
							  AND co.SchoolYear IN (2019,2020))
			   )
	        )
UNION ALL
----Total Subjects
SELECT CONCAT('     Analyzing: Total Subjects. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (
			    SELECT COUNT(DISTINCT AcademicSubjectDescriptor_CodeValue)
				FROM EdFiDW.[dbo].[DimCourse] 
				WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				  AND AcademicSubjectDescriptor_CodeValue <> 'N/A'
			   ),
			   '  ',
			   'Records in source system:', 
			   (
			    SELECT COUNT(DISTINCT ast.CodeValue) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Course c	 
	                 INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.AcademicSubjectType ast ON c.AcademicSubjectDescriptorId = ast.AcademicSubjectTypeId				     
				WHERE EXISTS (SELECT 1 
							  FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CourseOffering co 
							  WHERE c.CourseCode = co.CourseCode
							  AND co.SchoolYear IN (2019,2020))
			   )
	        )
UNION ALL
----Total GPA Applicability Types
SELECT CONCAT('     Analyzing: Total GPA Applicability Types. => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (
			    SELECT COUNT(DISTINCT GPAApplicabilityType_CodeValue)
				FROM EdFiDW.[dbo].[DimCourse] 
				WHERE CHARINDEX('Ed-Fi',_sourceKey,1) > 0
				  AND GPAApplicabilityType_CodeValue <> 'N/A'
			   ),
			   '  ',
			   'Records in source system:', 
			   (
			    SELECT COUNT(DISTINCT cgat.CodeValue) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Course c	 
	                 INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CourseGPAApplicabilityType cgat ON c.CourseGPAApplicabilityTypeId = cgat.CourseGPAApplicabilityTypeId
				WHERE EXISTS (SELECT 1 
							  FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CourseOffering co 
							  WHERE c.CourseCode = co.CourseCode
							  AND co.SchoolYear IN (2019,2020))
			   )
	        )


/*
--FactStudentAttendanceByDay
------------------------------------------------------------------------------------------
UNION ALL
SELECT ' --------------------------------------------------------------------------------------------------------------------------------------'
UNION ALL
SELECT ' Entity being analyzed: [FactStudentAttendanceByDay]'
UNION ALL
----Total
SELECT CONCAT('     Analyzing: Total Attendance Events (Including In Attendance). => Source: Ed-Fi ODS => ',
			   'Records in entity:',
			   (
			    SELECT COUNT(DISTINCT ds._sourceKey)
				FROM EdFiDW.[dbo].[FactStudentAttendanceByDay]  fsabd 
				     INNER JOIN EdFiDW.[dbo].DimStudent ds ON fsabd .StudentKey = ds.StudentKey
					 INNER JOIN EdFiDW.[dbo].DimTime dt ON fsabd.TimeKey = dt.TimeKey
				WHERE CHARINDEX('Ed-Fi',fsabd._sourceKey,1) > 0
				AND EXISTS (SELECT 1  
				            FROM EdFiDW.[dbo].DimAttendanceEventCategory daec
							WHERE fsabd.AttendanceEventCategoryKey = daec.AttendanceEventCategoryKey
							  AND daec.InAttendance_Indicator = 1)
			   ),
			   '  ',
			   'Records in source system:',
			   (
			    SELECT COUNT(DISTINCT d.DescriptorId) 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d			     
				WHERE d.Namespace IN ('http://ed-fi.org/Descriptor/AttendanceEventCategoryDescriptor.xml',
				                      'http://ed-fi.org/Descriptor/Follett/Aspen/AttendanceEventCategoryDescriptor.xml')
			   )
	        )
SELECT * FROM EdFiDW.[dbo].[FactStudentAttendanceByDay]
SELECT * FROM EdFiDW.[dbo].DimTime
*/