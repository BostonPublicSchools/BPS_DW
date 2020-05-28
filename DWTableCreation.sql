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
  SchoolCategoryType_Elementary BIT NOT NULL,      
  SchoolCategoryType_Middle BIT NOT NULL,
  SchoolCategoryType_HighSchool BIT NOT NULL,    
  SchoolCategoryType_Combined BIT NOT NULL,    
  
  SchoolGradeLevel_Lowest_Descriptor_CodeValue NVARCHAR(50) NULL, -- Ninth grade
  SchoolGradeLevel_Highest_Descriptor_CodeValue  NVARCHAR(1024) NULL, -- Twelfth grade

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
  
  AYP_Indicator BIT NULL, -- True,False -- Edfi ?

  
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
    
  SchoolDay DATE NOT NULL , -- 9/1/2019
  
  --all these vary by school
  SchoolKey INT NULL,  
  InstructionnalDay NVARCHAR(50) NOT NULL, -- InstructionalDay, Non-Instructional Day
  InstructionnalDayType NVARCHAR(50) NOT NULL, -- Full-Day, Partial-Day, Early-Release Day, Make-Up Day
  InstructionnalDayType_FullDay BIT NOT NULL, 
  InstructionnalDayType_PartialDay BIT NOT NULL, 
  InstructionnalDayType_EarlyRelease BIT NOT NULL, 
  InstructionnalDayType_MakeUpDay BIT NOT NULL, 
  BlockScheduleDay NVARCHAR(50) NOT NULL, -- A-Day, B-day
  BlockScheduleDay_ADay BIT NOT NULL, -- Y, N
  BlockScheduleDay_BDay BIT NOT NULL, -- Y, N
  Semester INT NULL, -- 1,2
  SemesterCode NVARCHAR(50) NULL, -- S1,S1
  SemesterDescription NVARCHAR(50) NULL, -- Semester 1, Semester 2
  Trimester INT NULL, -- 1,2,3
  TrimesterCode NVARCHAR(50) NULL, -- T1,T2
  TrimesterDescription NVARCHAR(50) NULL, -- Timester 1, Timester 2, Timester 3
  [Quarter] INT NULL, -- 1,2,3,4
  QuarterCode NVARCHAR(50) NULL, -- Q1,Q2,Q3,Q4
  QuarterDescription NVARCHAR(50) NULL, -- Quarter 1, Quarter 2, Quarter 3, Quarter 4



  [Weekend_Indicator] BIT NOT NULL,
  [Holiday_Indicator] BIT NOT NULL, --  1,0
  [HolidayName] NVARCHAR(20) NOT NULL, -- Memorial Day
  [SpecialDay] NVARCHAR(20) NOT NULL, --  Valentine Day


  WeekBeforeChristmas_Indicator BIT NOT NULL, --  Y, N

  StateExaminationPeriod_Indicator BIT NOT NULL, --  True,False

  SchoolYear INT NOT NULL, -- ex: 9/1/2019 = 2020
  SchoolYearDescription NVARCHAR(50) NOT NULL, -- '2019-2020 or SchoolYear 2019 - 2020'
  
  [Month] TINYINT NOT NULL, -- 1..12
  [MonthName]  NVARCHAR(10) NOT NULL, --January,February,December
  [MonthNameShort]  CHAR(3) NOT NULL, --Jan,Feb,Dec
  [MonthNameFirstLetter]  CHAR(1) NOT NULL, --J,F,D

  [DayOfMonth]  int NOT NULL, -- 1 - 30|31
  [DayOfWeek]  int NOT NULL, -- 1 -7
  [DayName]  NVARCHAR(15) NOT NULL, -- Monday, Tuesday
   
  CalendarYear INT NOT NULL, -- ex: 9/1/2019 = 2019

 
  

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
  CurrentAge INT NOT NULL,
  SchoolYearAdmitted INT NOT NULL,
  Graduation_Indicator BIT NULL,
  
  Homeroom  NVARCHAR(50) NULL,
  HomeroomTeacher NVARCHAR(100) NULL,

  SexType_Code NVARCHAR(15) NOT NULL,
  SexType_Description NVARCHAR(100) NOT NULL,    
  SexType_Male_Indicator BIT NOT NULL,
  SexType_Female_Indicator BIT NOT NULL,
  SexType_NotSelected_Indicator BIT NOT NULL,
  
  RaceCode NVARCHAR(15) NOT NULL,
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
    
  IEP_Indicator BIT NOT NULL, -- edfi.Program?
  
  LimitedEnglishProficiencyDescriptor_CodeValue NVARCHAR(25) NOT NULL, -- L, F, N 
  LimitedEnglishProficiencyDescriptor_Description NVARCHAR(60) NOT NULL,  -- English Learner , Former, Neither  

  LimitedEnglishProficiency_EnglishLearner_Indicator BIT NOT NULL,
  LimitedEnglishProficiency_Former_Indicator BIT NOT NULL,
  LimitedEnglishProficiency_NotEnglisLearner_Indicator BIT NOT NULL,
  
  EconomicDisadvantage_Indicator BIT NOT NULL,   

  EntryGradeLevelReasonType_CodeValue NVARCHAR(50) NOT NULL, -- Promotion - Variable progress, Promotion - Other, etc
  EntryGradeLevelReasonType_Description NVARCHAR(1024) NOT NULL,
  EntryGradeLevelReasonType_PromotionVariableProgress_Indicator BIT NOT NULL,
  EntryGradeLevelReasonType_PromotionOther_Indicator BIT NOT NULL,
  EntryGradeLevelReasonType_NonpromotionFailedToMeetTestingRequirements_Indicator BIT NOT NULL,
  EntryGradeLevelReasonType_NonpromotionIllness_Indicator BIT NOT NULL,
  EntryGradeLevelReasonType_NonpromotionInsufficientCredits_Indicator BIT NOT NULL,
  EntryGradeLevelReasonType_NonpromotionProlongedAbsence_Indicator BIT NOT NULL,
  EntryGradeLevelReasonType_NonpromotionImmaturity_Indicator BIT NOT NULL,
    
  EntryDate DATETIME2 NOT NULL,
  EntrySchoolYear INT NOT NULL,
  EntryCode NVARCHAR(25) NOT NULL, 
  ExitWithdrawDate  DATETIME2 NULL,
  ExitWithdrawSchoolYear INT NOT NULL,
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
  TimeKey INT NOT NULL, -- this could be a date, or a specific period
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
  TimeKey INT NOT NULL, -- this could be a date, or a specific period  
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
  TimeKey INT NOT NULL, -- this could be a date, or a specific period
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
  TimeKey INT NOT NULL, -- this could be a date, or a specific period
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



