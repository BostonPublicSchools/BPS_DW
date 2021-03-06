DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM EdFiDW.[dbo].[Lineage] WHERE TableName= 'dbo.FactStudentCourseTranscript')
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
	('dbo.FactStudentCourseTranscript', 
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
	 WHERE TableName= 'dbo.FactStudentCourseTranscript'
END 

;WITH SchoolTermsFirstDates AS 
(
  SELECT DISTINCT SchoolKey, 
                  SchoolTermDescriptor_CodeValue AS Term,
				  SchoolYear,
                  MIN(SchoolDate) OVER (PARTITION BY SchoolKey, SchoolTermDescriptor_CodeValue, SchoolYear) AS MinTermDate
  FROM EdFiDW.dbo.DimTime 
  WHERE SchoolKey IS NOT NULL

)


INSERT INTO EdFiDW.[dbo].[FactStudentCourseTranscript]
           ([StudentKey]
           ,[TimeKey]
           ,[CourseKey]
		   ,[SchoolKey]
           ,EarnedCredits
           ,PossibleCredits 
           ,FinalLetterGradeEarned
		   ,FinalNumericGradeEarned
           ,[LineageKey])
		    
SELECT distinct
      ds.StudentKey,
      dt.TimeKey,
	  dcourse.CourseKey,	  
	  dschool.SchoolKey,      
	  COALESCE(scg.CreditsEarned,0) AS EarnedCredits,
	  scg.CreditsPossible AS PossibleCredits,
	  CASE WHEN scg.CreditsEarned = 0 AND scg.FinalMark IS NULL THEN 'NC'
	  ELSE
		  CASE WHEN TRY_CAST(scg.FinalMark AS DECIMAL) IS NULL THEN scg.FinalMark 
			ELSE NULL 
	      END 
	  END AS FinalLetterGradeEarned,
	  CASE WHEN TRY_CAST(scg.FinalMark AS DECIMAL) IS NOT NULL THEN scg.FinalMark ELSE NULL END AS FinalNumericGradeEarned,
	  --dt.SchoolDate, *
	  @lineageKey AS [LineageKey]
--select *  
FROM [BPSGranary02].[RAEDatabase].[dbo].[StudentCourseGrade_aspenNewFormat] scg
    
	--joining DW tables
	INNER JOIN EdFiDW.dbo.DimStudent ds  ON CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),scg.StudentNo))  = ds._sourceKey
	      
	INNER JOIN EdFiDW.dbo.DimSchool dschool ON CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),scg.SchoolID))   = dschool._sourceKey
	INNER JOIN EdFiDW.dbo.DimCourse dcourse ON CONCAT('LegacyDW|',scg.CourseNumber,'-',CASE WHEN scg.SectionID = '' THEN 'N/A' ELSE scg.SectionID END)  = dcourse._sourceKey	
	INNER JOIN EdFiDW.dbo.DimTime dt on dt.SchoolKey is not null   
									and dschool.SchoolKey = dt.SchoolKey
                                    and CASE WHEN scg.Semester IN ('A','MS Pre-Algebra','Advisory') AND dt.SchoolTermDescriptor_CodeValue = 'Other' THEN 1									         
											 WHEN scg.Semester IN ('SS') AND dt.SchoolTermDescriptor_CodeValue = 'Summer Semester' THEN 1
											 WHEN scg.Semester IN ('1') AND dt.SchoolTermDescriptor_CodeValue = 'Fall Semester' THEN 1
											 WHEN scg.Semester IN ('2') AND dt.SchoolTermDescriptor_CodeValue = 'Spring Semester' THEN 1
										
											 WHEN scg.Semester IN ('Q1') AND dt.SchoolTermDescriptor_CodeValue = 'First Quarter' THEN 1
											 WHEN scg.Semester IN ('Q123','Q13','Q14','Q23','Q24','T124','T234') AND dt.SchoolTermDescriptor_CodeValue = 'Other' THEN 1											 
											 WHEN scg.Semester IN ('Q2') AND dt.SchoolTermDescriptor_CodeValue = 'Second Quarter' THEN 1
											 WHEN scg.Semester IN ('Q3') AND dt.SchoolTermDescriptor_CodeValue = 'Third Quarter' THEN 1
											 WHEN scg.Semester IN ('Q4','T4') AND dt.SchoolTermDescriptor_CodeValue = 'Fourth Quarter' THEN 1											 

											 

											 WHEN scg.Semester IN ('T1') AND scg.SchoolID IN ('4580','1064','1420') AND dt.SchoolTermDescriptor_CodeValue = 'First Quarter' THEN 1
											 WHEN scg.Semester IN ('T1') AND dt.SchoolTermDescriptor_CodeValue = 'First Trimester' THEN 1

											 WHEN scg.Semester IN ('T2') AND scg.SchoolID IN ('4580','1064','1420') AND dt.SchoolTermDescriptor_CodeValue = 'Second Quarter' THEN 1
											 WHEN scg.Semester IN ('T2') AND dt.SchoolTermDescriptor_CodeValue = 'Second Trimester' THEN 1

											 WHEN scg.Semester IN ('T3') AND scg.SchoolID IN ('4580','1064','1420') AND dt.SchoolTermDescriptor_CodeValue = 'Third Quarter' THEN 1
											 WHEN scg.Semester IN ('T3') AND dt.SchoolTermDescriptor_CodeValue = 'Third Trimester' THEN 1
											 ELSE 0
									    END = 1
									AND RIGHT(RTRIM(scg.SchoolYear),4) = dt.SchoolYear

WHERE   dt.SchoolDate BETWEEN ds.ValidFrom  AND ds.ValidTo
    AND dt.SchoolDate BETWEEN dschool.ValidFrom AND dschool.ValidTo
    AND dt.SchoolDate BETWEEN dcourse.ValidFrom AND dcourse.ValidTo
    AND scg.SchoolYear IN ('2015-2016','2016-2017', '2017-2018')	   
    AND scg.FinalMark IS NOT NULL
	 AND EXISTS (SELECT 1
               FROM SchoolTermsFirstDates std 
			   WHERE dschool.SchoolKey = std.SchoolKey
			     AND dt.SchoolTermDescriptor_CodeValue = std.Term
				 AND dt.SchoolYear = std.SchoolYear
				 AND dt.SchoolDate = std.MinTermDate)
     AND COALESCE(scg.CreditsEarned,0) > 0


--select * from EdFiDW.[dbo].[FactStudentCourseTranscript]

--updatng the lineage table
UPDATE EdFiDW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;
/*
SELECT * FROM EdFiDW.dbo.DimStudent WHERE StudentKey = 352007
SELECT * FROM EdFiDW.dbo.DimTime WHERE TimeKey = 107063
SELECT * FROM EdFiDW.dbo.DimCourse WHERE CourseKey = 13935
SELECT * FROM EdFiDW.dbo.DimSchool WHERE SchoolKey =  38

select DISTINCT scg.*
FROM [BPSGranary02].[RAEDatabase].[dbo].[StudentCourseGrade_aspenNewFormat] scg 
  --WHERE  scg.SchoolYear IN ('2015-2016','2016-2017', '2017-2018')	 AND  (scg.CreditsEarned IS NULL OR scg.CreditsEarned = 0 )
    WHERE scg.StudentNo = '232864' AND scg.SchoolYear = '2017-2018' AND scg.CourseNumber = '566' and SectionId = '4R23'



SELECT * FROM EdFiDW.dbo.DimStudent WHERE StudentKey = 922582
SELECT * FROM EdFiDW.dbo.DimTime WHERE TimeKey = 85661
SELECT * FROM EdFiDW.dbo.DimCourse WHERE CourseKey = 691
SELECT * FROM EdFiDW.dbo.DimSchool WHERE SchoolKey =  516

SELECT * FROM EdFiDW.dbo.DimStudent WHERE StudentKey = 484502
SELECT * FROM EdFiDW.dbo.DimTime WHERE TimeKey = 107100
SELECT * FROM EdFiDW.dbo.DimCourse WHERE CourseKey = 5462
SELECT * FROM EdFiDW.dbo.DimSchool WHERE SchoolKey =  517

SELECT * FROM EdFiDW.dbo.DimStudent WHERE StudentKey = 28717
SELECT * FROM EdFiDW.dbo.DimTime WHERE SchoolKey =  516  and SchoolYear = 2018 and SchoolTermDescriptor_CodeValue = 'Fall Semester' order by SchoolDate TimeKey = 20943
SELECT * FROM EdFiDW.dbo.DimCourse WHERE _sourceKey like '%152-11%' CourseKey = 75840
SELECT * FROM EdFiDW.dbo.DimSchool WHERE SchoolKey =  516

select DISTINCT scg.*
FROM [BPSGranary02].[RAEDatabase].[dbo].[StudentCourseGrade_aspenNewFormat] scg 
  --WHERE  scg.SchoolYear IN ('2015-2016','2016-2017', '2017-2018')	 AND  (scg.CreditsEarned IS NULL OR scg.CreditsEarned = 0 )
    WHERE scg.StudentNo = '260786' AND scg.SchoolYear = '2017-2018' AND scg.CourseNumber = '152' and SectionId = '11'


select DISTINCT scg.*
FROM [BPSGranary02].[RAEDatabase].[dbo].[StudentCourseGrade_aspenNewFormat] scg 
  --WHERE  scg.SchoolYear IN ('2015-2016','2016-2017', '2017-2018')	 AND  (scg.CreditsEarned IS NULL OR scg.CreditsEarned = 0 )
    WHERE scg.StudentNo = '380895' AND scg.SchoolYear = '2017-2018' AND scg.CourseNumber = '454' and SectionId = '1B4'


select DISTINCT *
FROM [BPSGranary02].[RAEDatabase].[dbo].[StudentCourseGrade_aspenNewFormat] scg 
 WHERE  scg.SchoolYear IN ('2015-2016','2016-2017', '2017-2018')--	 AND  (scg.CreditsEarned IS NULL OR scg.CreditsEarned = 0 )
    WHERE scg.StudentNo = '210734' AND scg.SchoolYear = '2017-2018' AND scg.CourseNumber = '624'

select DISTINCT *
FROM [BPSGranary02].[RAEDatabase].[dbo].[StudentCourseGrade_aspenNewFormat] scg 
    WHERE scg.StudentNo = '232864' AND scg.SchoolYear = '2017-2018' AND scg.CourseNumber = '566'

select DISTINCT Semester
FROM [BPSGranary02].[RAEDatabase].[dbo].[StudentCourseGrade_aspenNewFormat] scg 
 WHERE  scg.SchoolYear IN ('2015-2016','2016-2017', '2017-2018')--	 AND  (scg.CreditsEarned IS NULL OR scg.CreditsEarned = 0 )
 
select DISTINCT *
FROM [BPSGranary02].[RAEDatabase].[dbo].[StudentCourseGrade_aspenNewFormat] scg 
 WHERE  scg.SchoolYear IN ('2015-2016','2016-2017', '2017-2018') and Semester = ''
 
select DISTINCT Semester
FROM [BPSGranary02].[RAEDatabase].[dbo].[StudentCourseGrade_aspenNewFormat] scg 
 WHERE  scg.SchoolYear IN ('2015-2016','2016-2017', '2017-2018')--	 AND  (scg.CreditsEarned IS NULL OR scg.CreditsEarned = 0 )
 
select DISTINCT *
FROM [BPSGranary02].[RAEDatabase].[dbo].[StudentCourseGrade_aspenNewFormat] scg 
 WHERE  scg.SchoolYear IN ('2015-2016','2016-2017', '2017-2018') and Semester = 'Q123'
 and StudentNo = '228516'

SELECT DISTINCT 
		   --ses.SessionName,
		   td.CodeValue TermDescriptorCodeValue,
		   td.Description TermDescriptorDescription,
		   ses.SessionName
	FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School s
		INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganization edOrg  ON s.SchoolId = edOrg.EducationOrganizationId
		INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CalendarDate cd ON s.SchoolId = cd.SchoolId
		INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CalendarDateCalendarEvent cdce ON cd.SchoolId = cdce.SchoolId
																							AND cd.Date = cdce.Date
		INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CalendarEventDescriptor ced  ON cdce.CalendarEventDescriptorId = ced.CalendarEventDescriptorId
		INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor cedv  ON ced.CalendarEventDescriptorId = cedv.DescriptorId
		INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CalendarEventType cet ON ced.CalendarEventTypeId = cet.CalendarEventTypeId
		INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Session ses ON s.SchoolId = ses.SchoolId
																		 AND cd.Date BETWEEN ses.BeginDate AND ses.EndDate
		INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor td ON ses.TermDescriptorId = td.DescriptorId
		ORDER BY td.CodeValue
		
*/





