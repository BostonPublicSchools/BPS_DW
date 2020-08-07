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

--truncate table EdFiDW.[dbo].FactStudentCourseTranscript

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
		    
SELECT DISTINCT 
      ds.StudentKey,
      dt.TimeKey,
	  dcourse.CourseKey,	  
	  dschool.SchoolKey,      
	  scg.CreditsEarned AS EarnedCredits,
	  scg.CreditsPossible AS PossibleCredits,
	  CASE WHEN scg.CreditsEarned = 0 THEN 'NC'
	  ELSE
		  CASE WHEN TRY_CAST(scg.FinalMark AS DECIMAL) IS NULL THEN scg.FinalMark 
			ELSE NULL 
	      END 
	  END AS FinalLetterGradeEarned,
	  CASE WHEN TRY_CAST(scg.FinalMark AS DECIMAL) IS NOT NULL THEN scg.FinalMark ELSE NULL END AS FinalNumericGradeEarned,
	  @lineageKey AS [LineageKey]
--select *  
FROM [BPSGranary02].[RAEDatabase].[dbo].[StudentCourseGrade_aspenNewFormat] scg
    
	--joining DW tables
	INNER JOIN EdFiDW.dbo.DimStudent ds  ON CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),scg.StudentNo))  = ds._sourceKey
	INNER JOIN EdFiDW.dbo.DimSchool dschool ON CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),scg.SchoolID))   = dschool._sourceKey
	INNER JOIN EdFiDW.dbo.DimCourse dcourse ON CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),scg.CourseNumber))  = dcourse._sourceKey
	
	INNER JOIN EdFiDW.dbo.DimTime dt on dt.SchoolKey is not null   
									and dschool.SchoolKey = dt.SchoolKey
                                    and dt.SchoolTermDescriptor_CodeValue = 'Other' -- year
									AND RIGHT(RTRIM(scg.SchoolYear),4) = dt.SchoolYear

WHERE  dt.SchoolDate BETWEEN ds.ValidFrom  AND ds.ValidTo
   --AND dt.SchoolDate <= GETDATE()   
   AND scg.SchoolYear IN ('2015-2016','2016-2017', '2017-2018')	
      


--select * from EdFiDW.[dbo].[FactStudentCourseTranscript]

--updatng the lineage table
UPDATE EdFiDW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;


