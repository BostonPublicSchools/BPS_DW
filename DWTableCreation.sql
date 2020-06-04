CREATE TABLE dbo.Lineage
(
   LineageKey INT NOT NULL IDENTITY(1,1), -- surrogate
   TableName NVARCHAR(100) NOT NULL,
   StartTime DATETIME NOT NULL,
   EndTime DATETIME NULL,
   LoadType CHAR(1) NOT NULL , -- F = Full, I = Incremental
   [Status] CHAR(1) NOT NULL, -- P = Processing , S = Success
   CONSTRAINT PK_Lineage PRIMARY KEY (LineageKey),
);

CREATE TABLE dbo.DimSchool
(
  SchoolKey int NOT NULL IDENTITY(1,1), -- ex 9/1/2019 : 20190901 -- surrogate
  [_sourceKey] NVARCHAR(50) NOT NULL,  --'ODS|Id'
  
  ShortNameOfInstitution NVARCHAR(15) NOT NULL,     
  NameOfInstitution NVARCHAR(100) NOT NULL,    

  SchoolCategoryType NVARCHAR(100) NOT NULL,     -- elem, middle, hs, combined
  SchoolCategoryType_Elementary_Indicator BIT NOT NULL,      
  SchoolCategoryType_Middle_Indicator BIT NOT NULL,
  SchoolCategoryType_HighSchool_Indicator BIT NOT NULL,    
  SchoolCategoryType_Combined_Indicator BIT NOT NULL,    
  
  SchoolGradeLevel_AdultEducation_Indicator  BIT NOT NULL, -- True,False
  SchoolGradeLevel_EarlyEducation_Indicator  BIT NOT NULL, -- True,False
  SchoolGradeLevel_Eighthgrade_Indicator  BIT NOT NULL, -- True,False
  SchoolGradeLevel_Eleventhgrade_Indicator  BIT NOT NULL, -- True,False
  SchoolGradeLevel_Fifthgrade_Indicator  BIT NOT NULL, -- True,False
  SchoolGradeLevel_Firstgrade_Indicator  BIT NOT NULL, -- True,False
  SchoolGradeLevel_Fourthgrade_Indicator  BIT NOT NULL, -- True,False
  SchoolGradeLevel_Grade13_Indicator  BIT NOT NULL, -- True,False
  SchoolGradeLevel_Infanttoddler_Indicator  BIT NOT NULL, -- True,False
  SchoolGradeLevel_Kindergarten_Indicator  BIT NOT NULL, -- True,False
  SchoolGradeLevel_Ninthgrade_Indicator  BIT NOT NULL, -- True,False
  SchoolGradeLevel_Other_Indicator  BIT NOT NULL, -- True,False
  SchoolGradeLevel_Postsecondary_Indicator  BIT NOT NULL, -- True,False
  SchoolGradeLevel_PreschoolPrekindergarten_Indicator  BIT NOT NULL, -- True,False
  SchoolGradeLevel_Secondgrade_Indicator  BIT NOT NULL, -- True,False
  SchoolGradeLevel_Seventhgrade_Indicator  BIT NOT NULL, -- True,False
  SchoolGradeLevel_Sixthgrade_Indicator  BIT NOT NULL, -- True,False
  SchoolGradeLevel_Tenthgrade_Indicator  BIT NOT NULL, -- True,False
  SchoolGradeLevel_Thirdgrade_Indicator  BIT NOT NULL, -- True,False
  SchoolGradeLevel_Twelfthgrade_Indicator  BIT NOT NULL, -- True,False
  SchoolGradeLevel_Ungraded_Indicator  BIT NOT NULL, -- True,False

  TitleIPartASchoolDesignationTypeCodeValue  NVARCHAR(50) NOT NULL,--Not designated as a Title I Part A school
																	--Title I Part A Schoolwide Assistance Program Schoo
																	--Title I Part A Targeted Assistance School
																	--Title I targeted eligible school - no program
																	--Title I targeted school
																	--Title I school wide eligible - Title I targeted pr
																	--Title I school wide eligible school - no program
                                            
  TitleIPartASchoolDesignation_Indicator BIT NOT NULL, -- True,False
  
  ValidFrom DATETIME NOT NULL,
  ValidTo DATETIME NOT NULL,
  IsCurrent BIT NOT NULL,  
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimSchool PRIMARY KEY (SchoolKey),
  CONSTRAINT FK_DimSchool_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);


CREATE TABLE dbo.DimTime
(
  TimeKey INT NOT NULL IDENTITY(1,1), -- ex 9/1/2019 : 20190901 -- surrogate    
  
  SchoolDate DATE NOT NULL , -- 9/1/2019
  SchoolDate_MMYYYY CHAR(6) NOT NULL,
  SchoolDate_Fomat1 CHAR(10) NOT NULL,
  SchoolDate_Fomat2 CHAR(8) NOT NULL,
  SchoolDate_Fomat3 CHAR(10) NOT NULL,
    
  SchoolYear SMALLINT NOT NULL, -- ex: 9/1/2019 = 2020  
  SchoolYearDescription NVARCHAR(50) NOT NULL, -- '2019-2020 or SchoolYear 2019 - 2020'
  CalendarYear SMALLINT NOT NULL, -- ex: 9/1/2019 = 2020
  
  DayOfMonth TINYINT NOT NULL, -- 1 - 30|31
  DaySuffix CHAR(2) NOT NULL , -- 1st, 2nd, 3rd
  DayName  NVARCHAR(15) NOT NULL, -- Monday, Tuesday    
  DayNameShort NVARCHAR(15) NOT NULL, -- Mon, Tue
  DayOfWeek TINYINT NOT NULL, -- 1 - 7
  
  WeekInMonth TINYINT NOT NULL, -- 1 - 4 or 5 -- this counts 7 days starting on the 1st of the month
  WeekOfMonth TINYINT NOT NULL, -- 1 - 4 or 5 -- this is the actual week of the month (starting on sunday)
  Weekend_Indicator BIT NOT NULL,
  WeekOfYear TINYINT NOT NULL, -- 1 - 53   
  FirstDayOfWeek DATE NOT NULL,
  LastDayOfWeek DATE NOT NULL,
  WeekBeforeChristmas_Indicator BIT NOT NULL, --  True,False
  
  
  [Month] TINYINT NOT NULL, -- 1..12
  MonthName  NVARCHAR(10) NOT NULL, --January,February,December
  MonthNameShort CHAR(3) NOT NULL, --Jan,Feb,Dec  
  FirstDayOfMonth DATE NOT NULL,
  LastDayOfMonth DATE NOT NULL,
  FirstDayOfNextMonth DATE NOT NULL,
  LastDayOfNextMonth DATE NOT NULL,
    
  DayOfYear SMALLINT NOT NULL, -- 1 - 365 or 366 (Leap Year Every Four Years)  
  DayOfSchoolYear SMALLINT NOT NULL, -- 1 - 180 - based on SIS(ODS) school calendar *****************************************change********************************
  LeapYear_Indicator BIT NOT NULL,  
    
  FederalHolidayName NVARCHAR(20) NULL, -- Memorial Day
  FederalHoliday_Indicator BIT NOT NULL, --  True,False
  
  --all these vary by school
  SchoolKey INT NULL,  
  
  SchoolCalendarEventType_CodeValue NVARCHAR(50) NULL, -- Emergency day,Instructional day,Teacher only day
  SchoolCalendarEventType_Description NVARCHAR(50) NULL, -- Emergency day,Instructional day,Teacher only day
    
  SchoolTermDescriptor_CodeValue NVARCHAR(50) NULL, -- Emergency day,Instructional day,Teacher only day
  SchoolTermDescriptor_Description NVARCHAR(50) NULL, -- Emergency day,Instructional day,Teacher only day

  --all indicators were removed until we see actual district's data.   
  
  ValidFrom DATETIME NOT NULL,
  ValidTo DATETIME NOT NULL,
  IsCurrent BIT NOT NULL,  
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimTime PRIMARY KEY (TimeKey),  
  CONSTRAINT FK_DimTime_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES [dbo].[DimSchool] (SchoolKey),
  CONSTRAINT FK_DimTime_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);


CREATE TABLE dbo.DimStudent
(
  StudentKey INT NOT NULL IDENTITY(1,1),  -- surrogate
  [_sourceKey] NVARCHAR(50) NOT NULL,

  StudentUniqueId NVARCHAR(32) NULL, -- district 
  StateId NVARCHAR(32) NULL, -- state
  
  SchoolKey INT NOT NULL,
  ShortNameOfInstitution NVARCHAR(10) NOT NULL,
  NameOfInstitution NVARCHAR(100) NOT NULL,
  GradeLevelDescriptor_CodeValue NVARCHAR(100) NOT NULL,
  GradeLevelDescriptor_Description NVARCHAR(500) NOT NULL,
  
  FirstName NVARCHAR(50) NOT NULL,
  MiddleInitial CHAR(1) NULL,
  MiddleName NVARCHAR(50) NULL,
  LastSurname NVARCHAR(50) NOT NULL,
  FullName NVARCHAR(50) NOT NULL,
  BirthDate DATE NOT NULL,
  StudentAge INT NOT NULL,  
  GraduationSchoolYear INT NOT NULL,  
  
  Homeroom  NVARCHAR(50) NULL,
  HomeroomTeacher NVARCHAR(100) NULL,

  SexType_Code NVARCHAR(15) NOT NULL,
  SexType_Description NVARCHAR(100) NOT NULL,    
  SexType_Male_Indicator BIT NOT NULL,
  SexType_Female_Indicator BIT NOT NULL,
  SexType_NotSelected_Indicator BIT NOT NULL,
  
  RaceCode NVARCHAR(50) NOT NULL,
  RaceDescription NVARCHAR(100) NOT NULL,    
  Race_AmericanIndianAlaskanNative_Indicator BIT NOT NULL,
  Race_Asian_Indicator BIT NOT NULL,
  Race_BlackAfricaAmerican_Indicator BIT NOT NULL,
  Race_NativeHawaiianPacificIslander_Indicator BIT NOT NULL,
  Race_White_Indicator BIT NOT NULL,
  Race_MultiRace_Indicator BIT NOT NULL,
  Race_ChooseNotRespond_Indicator BIT NOT NULL,
  Race_Other_Indicator BIT NOT NULL,
	
  EthnicityCode NVARCHAR(15) NOT NULL,
  EthnicityDescription NVARCHAR(100) NOT NULL,
  EthnicityHispanicLatino_Indicator BIT NOT NULL,
    
  Migrant_Indicator BIT NOT NULL,
  Homeless_Indicator BIT NOT NULL,
  IEP_Indicator BIT NOT NULL,
  
  LimitedEnglishProficiencyDescriptor_CodeValue NVARCHAR(25) NOT NULL, -- L, F, N 
  LimitedEnglishProficiencyDescriptor_Description NVARCHAR(60) NOT NULL,  -- English Learner , Former, Neither  

  LimitedEnglishProficiency_EnglishLearner_Indicator BIT NOT NULL,
  LimitedEnglishProficiency_Former_Indicator BIT NOT NULL,
  LimitedEnglishProficiency_NotEnglisLearner_Indicator BIT NOT NULL,
  
  EconomicDisadvantage_Indicator BIT NOT NULL,   --need to review some of these flags. Some of them should allow nulls. Showing a value of 0 by defautl will not be accurate

    
  EntryDate DATETIME2 NOT NULL,
  EntrySchoolYear INT NOT NULL,
  EntryCode NVARCHAR(25) NOT NULL, 
  
  ExitWithdrawDate  DATETIME2 NULL,
  ExitWithdrawSchoolYear INT NULL,
  ExitWithdrawCode  NVARCHAR(25) NULL,
     
  ValidFrom DATETIME NOT NULL, 
  ValidTo DATETIME NOT NULL, 
  IsCurrent BIT NOT NULL,  
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimStudent PRIMARY KEY (StudentKey),
  CONSTRAINT FK_DimStudent_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES dbo.DimSchool(SchoolKey),
  CONSTRAINT FK_DimStudent_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);


CREATE TABLE dbo.DimAssessment
(
    AssessmentKey INT NOT NULL IDENTITY(1,1),  
	[_sourceKey] NVARCHAR(50) NOT NULL,
	
	AssessmentCategoryDescriptor_CodeValue NVARCHAR(50) NOT NULL,    
	AssessmentCategoryDescriptor_Description NVARCHAR(1024) NOT NULL,    
	AssessmentFamilyTitle NVARCHAR(100) NULL,    	
	AdaptiveAssessment_Indicator bit NOT NULL, 

	--assessment 
	--------------------------------------------------------------------------------------
	AssessmentIdentifier NVARCHAR(25) NOT NULL,   
	AssessmentTitle NVARCHAR(500) NOT NULL,
	--scores 
	AssessmentScore_AssessmentReportingMethod_DescriptorCodeValue NVARCHAR(50) NOT NULL,   
	AssessmentScore_AssessmentReportingMethod_DescriptorDescriptorDescription NVARCHAR(1024) NOT NULL,   
	
	AssessmentScore_ResultDatatypeType_DescriptorCodeValue  NVARCHAR(50) NOT NULL,   
	AssessmentScore_ResultDatatypeType_DescriptorDescription NVARCHAR(1024) NOT NULL,   
	
	--performanceLevels
	AssessmentPerformanceLevel_DescriptorCodeValue NVARCHAR(50) NOT NULL,   
	AssessmentPerformanceLevel_DescriptorDescriptorDescription NVARCHAR(1024) NOT NULL,   	
	
	AssessmentPerformanceLevel_AssessmentReportingMethodDescriptor_CodeValue NVARCHAR(50) NOT NULL,   
	PerformanceLevel_AssessmentReportingMethodDescriptor_DescriptorDescription NVARCHAR(1024) NOT NULL,   		

	AssessmentPerformanceLevel_ResultDatatypeType_DescriptorCodeValue  NVARCHAR(50) NOT NULL,   
	AssessmentPerformanceLevel_ResultDatatypeType_DescriptorDescription NVARCHAR(1024) NOT NULL,   	
	-----------------------------------------------------------------------------------------

	--parent objective assessment
	--------------------------------------------------------------------------------------
	ParentObjectiveAssessmentIdentificationCode NVARCHAR(60) NOT NULL,   
	ParentObjectiveAssessmentIdentificationDescription NVARCHAR(1024) NOT NULL,  
	--scores 
	ParentObjectiveAssessmentScore_AssessmentReportingMethod_DescriptorCodeValue NVARCHAR(50) NOT NULL,   
	ParentObjectiveAssessmentScore_AssessmentReportingMethod_DescriptorDescriptorDescription NVARCHAR(1024) NOT NULL,   
	
	ParentObjectiveAssessmentScore_ResultDatatypeType_DescriptorCodeValue  NVARCHAR(50) NOT NULL,   
	ParentObjectiveAssessmentScore_ResultDatatypeType_DescriptorDescription NVARCHAR(1024) NOT NULL,   
	
	--performanceLevels
	ParentObjectiveAssessmentPerformanceLevel_DescriptorCodeValue NVARCHAR(50) NOT NULL,   
	ParentObjectiveAssessmentPerformanceLevel_DescriptorDescriptorDescription NVARCHAR(1024) NOT NULL,   	
	
	ParentObjectiveAssessmentPerformanceLevel_AssessmentReportingMethodDescriptor_CodeValue NVARCHAR(50) NOT NULL,   
	ParentObjectivePerformanceLevel_AssessmentReportingMethodDescriptor_DescriptorDescription NVARCHAR(1024) NOT NULL,   		

	ParentObjectiveAssessmentPerformanceLevel_ResultDatatypeType_DescriptorCodeValue  NVARCHAR(50) NOT NULL,   
	ParentObjectiveAssessmentPerformanceLevel_ResultDatatypeType_DescriptorDescription NVARCHAR(1024) NOT NULL,  
	-----------------------------------------------------------------------------------------
	
	--child objective assessment
	--------------------------------------------------------------------------------------
	ChildObjectiveAssessmentIdentificationCode NVARCHAR(60) NOT NULL,   
	ChildObjectiveAssessmentIdentificationDescription NVARCHAR(1024) NOT NULL,  
	--scores 
	ChildObjectiveAssessmentScore_AssessmentReportingMethod_DescriptorCodeValue NVARCHAR(50) NOT NULL,   
	ChildObjectiveAssessmentScore_AssessmentReportingMethod_DescriptorDescriptorDescription NVARCHAR(1024) NOT NULL,   
	
	ChildObjectiveAssessmentScore_ResultDatatypeType_DescriptorCodeValue  NVARCHAR(50) NOT NULL,   
	ChildObjectiveAssessmentScore_ResultDatatypeType_DescriptorDescription NVARCHAR(1024) NOT NULL,   
	
	--performanceLevels
	ChildObjectiveAssessmentPerformanceLevel_DescriptorCodeValue NVARCHAR(50) NOT NULL,   
	ChildObjectiveAssessmentPerformanceLevel_DescriptorDescriptorDescription NVARCHAR(1024) NOT NULL,   	
	
	ChildObjectiveAssessmentPerformanceLevel_AssessmentReportingMethodDescriptor_CodeValue NVARCHAR(50) NOT NULL,   
	ChildObjectivePerformanceLevel_AssessmentReportingMethodDescriptor_DescriptorDescription NVARCHAR(1024) NOT NULL,   		

	ChildObjectiveAssessmentPerformanceLevel_ResultDatatypeType_DescriptorCodeValue  NVARCHAR(50) NOT NULL,   
	ChildObjectiveAssessmentPerformanceLevel_ResultDatatypeType_DescriptorDescription NVARCHAR(1024) NOT NULL,  
	-----------------------------------------------------------------------------------------
	
	
	
	ValidFrom DATETIME NOT NULL, 
	ValidTo DATETIME NOT NULL, 
	IsCurrent BIT NOT NULL,	
    [LineageKey] INT NOT NULL,

	CONSTRAINT PK_DimAssessment PRIMARY KEY (AssessmentKey),
    CONSTRAINT FK_DimAssessment_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);



CREATE TABLE dbo.DimCourse
(
    CourseKey INT NOT NULL IDENTITY(1,1),  --surrogate
	[_sourceKey] NVARCHAR(50) NOT NULL,
	
	CourseCode NVARCHAR(60) NOT NULL,
	CourseTitle NVARCHAR(100) NOT NULL,
	CourseDescription NVARCHAR(100) NOT NULL,

	AcademicSubjectDescriptor_CodeValue  NVARCHAR(50) NOT NULL,
	AcademicSubjectDescriptor_Description  NVARCHAR(1024) NOT NULL,
	AcademicSubjectDescriptor_Math_Indicator  BIT NOT NULL,
	AcademicSubjectDescriptor_ELA_Indicator  BIT NOT NULL,
	AcademicSubjectDescriptor_Science_Indicator  BIT NOT NULL,

	HighSchoolCourseRequirement_Indicator BIT NOT NULL,
	MinimumAvailableCredits INT NULL,
	MaximumAvailableCredits INT NULL,
			
	ValidFrom DATETIME NOT NULL, 
	ValidTo DATETIME NOT NULL, 
	IsCurrent BIT NOT NULL,	
    [LineageKey] INT NOT NULL,

  	CONSTRAINT PK_DimCourse PRIMARY KEY (CourseKey),
    CONSTRAINT FK_DimCourse_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);

/*Not needed  now. Student Dimension is answering all our questions right now
CREATE TABLE dbo.FactStudentRoster
(
  StudentKey INT NOT NULL,  
  ETLProcessedDateTime DATETIME NULL,
  CONSTRAINT FK_FactStudentRoster_PersonKey FOREIGN KEY (StudentKey) REFERENCES dbo.DimStudent(StudentKey)
)
*/

CREATE TABLE dbo.FactStudentBehavior
(
  StudentKey INT NOT NULL,
  TimeKey INT NOT NULL, 
  
  NumberOfISSIncidents INT NOT NULL,
  NumberOfOSSIncidents INT NOT NULL,

  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_FactStudentBehavior PRIMARY KEY (StudentKey ASC, TimeKey ASC),
  CONSTRAINT FK_FactStudentBehavior_StudentKey FOREIGN KEY (StudentKey) REFERENCES dbo.DimStudent(StudentKey),
  CONSTRAINT FK_FactStudentBehavior_TimeKey FOREIGN KEY (TimeKey) REFERENCES dbo.DimTime(TimeKey),
  CONSTRAINT FK_FactStudentBehavior_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);

CREATE TABLE dbo.FactStudentAttendance
(
  StudentKey INT NOT NULL,
  TimeKey INT NOT NULL,  
  NumberOfDaysPresent INT NOT NULL,
  NumberOfDaysAbsent INT NOT NULL,
  NumberOfDaysAbsentUnexcused INT NOT NULL,
  NumberOfDaysMembership INT NOT NULL,
  ADA INT NOT NULL,

  
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_FactStudentAttendance PRIMARY KEY (StudentKey ASC, TimeKey ASC),
  CONSTRAINT FK_FactStudentAttendance_StudentKey FOREIGN KEY (StudentKey) REFERENCES dbo.DimStudent(StudentKey),
  CONSTRAINT FK_FactStudentAttendance_TimeKey FOREIGN KEY (TimeKey) REFERENCES dbo.DimTime(TimeKey),
  CONSTRAINT FK_FactStudentAttendance_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
  
);

CREATE TABLE dbo.FactStudentAssessmentScore
(
  StudentKey INT NOT NULL,
  TimeKey INT NOT NULL, 
  AssessmentKey INT NOT NULL,

  Result NVARCHAR(35) NOT NULL,
    
  Score_AssessmentReportingMethodDescriptor_RawScore_Indicator BIT NOT NULL,
  Score_AssessmentReportingMethodDescriptor_ScaleScore_Indicator BIT NOT NULL,
  Score_AssessmentReportingMethodDescriptor_ProficiencyLevel_Indicator BIT NOT NULL,
  Score_AssessmentReportingMethodDescriptor_Percentile_Indicator BIT NOT NULL,
  
  Score_ResultDatatypeType_Level_Indicator BIT NOT NULL ,
  Score_ResultDatatypeType_Integer_Indicator BIT NOT NULL ,
  Score_ResultDatatypeType_Decimal_Indicator BIT NOT NULL ,
  Score_ResultDatatypeType_Percentage_Indicator BIT NOT NULL ,
  Score_ResultDatatypeType_Percentile_Indicator BIT NOT NULL ,
  Score_ResultDatatypeType_Range_Indicator BIT NOT NULL ,
  
  PerformanceLevel_Descriptor_Failing_Indicator BIT NOT NULL , -- grade 10 tests
  PerformanceLevel_Descriptor_Warning_Indicator BIT NOT NULL , -- grade 3-8 tests
  PerformanceLevel_Descriptor_NeedsImprovement_Indicator BIT NOT NULL ,  
  PerformanceLevel_Descriptor_Proficient_Indicator BIT NOT NULL ,
  PerformanceLevel_Descriptor_Advanced_Indicator BIT NOT NULL ,
      
  
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_FactStudentAssessmentScores PRIMARY KEY (StudentKey ASC, TimeKey ASC, AssessmentKey ASC),
  CONSTRAINT FK_FactStudentAssessmentScores_StudentKey FOREIGN KEY (StudentKey) REFERENCES dbo.DimStudent(StudentKey),
  CONSTRAINT FK_FactStudentAssessmentScores_TimeKey FOREIGN KEY (TimeKey) REFERENCES dbo.DimTime(TimeKey),
  CONSTRAINT FK_FactStudentAssessmentScore_TimeKey FOREIGN KEY (AssessmentKey) REFERENCES dbo.DimAssessment(AssessmentKey),
  CONSTRAINT FK_FactStudentAssessmentScore_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);


CREATE TABLE dbo.FactStudentCourseGrade
(
  StudentKey INT NOT NULL,
  TimeKey INT NOT NULL, 
  CourseKey INT NOT NULL,

  CreditsEarned INT NOT NULL,
  CreditsPossible INT NOT NULL,
  FinalMark NVARCHAR(10) NOT NULL,
    
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_FactStudentCourseGrade PRIMARY KEY (StudentKey ASC, TimeKey ASC, CourseKey ASC),
  CONSTRAINT FK_FactStudentCourseGrade_StudentKey FOREIGN KEY (StudentKey) REFERENCES dbo.DimStudent(StudentKey),
  CONSTRAINT FK_FactStudentCourseGrade_TimeKey FOREIGN KEY (TimeKey) REFERENCES dbo.DimTime(TimeKey),
  CONSTRAINT FK_FactStudentCourseGrade_CourseKey FOREIGN KEY (CourseKey) REFERENCES dbo.DimCourse(CourseKey) ,
  CONSTRAINT FK_FactStudentCourseGrade_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);



