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

CREATE TABLE dbo.DimAcademicCalendar
(
  AcademicCalendarKey int NOT NULL IDENTITY(1,1), -- surrogate
  [_sourceKey] NVARCHAR(50) NOT NULL,
  [AcademicPeriod]  NVARCHAR(50) NOT NULL,
  [AcademicPeriodDescription]  NVARCHAR(100) NOT NULL,
  [AcademicPeriodStart] DATE NOT NULL,
  [AcademicPeriodEnd] DATE NOT NULL,
  
  [SchoolYear] INT NOT NULL, -- ex: 9/1/2019 = 2020
  [CalendarYear] INT NOT NULL, -- ex: 9/1/2019 = 2019
  
  [ValidFrom] DATETIME NOT NULL,
  [ValidTo] DATETIME NOT NULL,
  [IsCurrent] BIT NOT NULL,
  --ETLProcessedDateTime DATETIME NULL,
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimAcademicCalendar PRIMARY KEY (AcademicCalendarKey),
  CONSTRAINT FK_DimAcademicCalendar_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey]),
);



CREATE TABLE dbo.DimSchool
(
  SchoolKey int NOT NULL IDENTITY(1,1), -- ex 9/1/2019 : 20190901 -- surrogate
  [_sourceKey] NVARCHAR(50) NOT NULL,  --'ODS|Id'
  
  SchoolCode NVARCHAR(15) NOT NULL,     
  SchoolName NVARCHAR(100) NOT NULL,    

  SchoolType NVARCHAR(100) NOT NULL,     -- elem, middle, hs, combined
  SchoolType_Elementary char(1) NOT NULL,      
  SchoolType_Middle char(1) NOT NULL,
  SchoolType_HighSchool char(1) NOT NULL,    
  SchoolType_Combined char(1) NOT NULL,    
  
  GradeLevelsServed NVARCHAR(100) NOT NULL,  -- KG,01,02,03,04,05

  GradeLevelsServed_Lowest CHAR(2) NOT NULL, -- KG
  GradeLevelsServed_Highest CHAR(2) NOT NULL, -- 05

  GradeLevelsServed_KGIndicator CHAR(1) NOT NULL, -- Y,N
  GradeLevelsServed_01Indicator CHAR(1) NOT NULL, -- Y,N
  GradeLevelsServed_02Indicator CHAR(1) NOT NULL, -- Y,N
  GradeLevelsServed_03Indicator CHAR(1) NOT NULL, -- Y,N
  GradeLevelsServed_04Indicator CHAR(1) NOT NULL, -- Y,N
  GradeLevelsServed_05Indicator CHAR(1) NOT NULL, -- Y,N
  GradeLevelsServed_06Indicator CHAR(1) NOT NULL, -- Y,N
  GradeLevelsServed_07Indicator CHAR(1) NOT NULL, -- Y,N
  GradeLevelsServed_08Indicator CHAR(1) NOT NULL, -- Y,N
  GradeLevelsServed_09Indicator CHAR(1) NOT NULL, -- Y,N
  GradeLevelsServed_10Indicator CHAR(1) NOT NULL, -- Y,N
  GradeLevelsServed_11Indicator CHAR(1) NOT NULL, -- Y,N
  GradeLevelsServed_12Indicator CHAR(1) NOT NULL, -- Y,N

  Title1Indicator CHAR(1) NOT NULL, -- Y,N
  AYPIndicator CHAR(1) NOT NULL, -- Y,N

  EffectiveStartDate DATETIME NOT NULL,
  EffectiveEndDate DATETIME NOT NULL,
  IsCurrent BIT NOT NULL,
  --ETLProcessedDateTime DATETIME NULL,
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimSchool PRIMARY KEY (SchoolKey),
  CONSTRAINT FK_DimSchool_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);


CREATE TABLE dbo.DimTime
(
  TimeKey INT NOT NULL IDENTITY(1,1), -- ex 9/1/2019 : 20190901 -- surrogate
  AcademicCalendarKey INT NOT NULL,
  
  SchoolDay DATE NOT NULL , -- 9/1/2019
  
  --all these vary by school
  SchoolKey INT NULL,  
  InstructionnalDay NVARCHAR(50) NOT NULL, -- InstructionalDay, Non-Instructional Day
  InstructionnalDayType NVARCHAR(50) NOT NULL, -- Full-Day, Partial-Day, Early-Release Day, Make-Up Day
  InstructionnalDayType_FullDay CHAR(1) NOT NULL, 
  InstructionnalDayType_PartialDay CHAR(1) NOT NULL, 
  InstructionnalDayType_EarlyRelease CHAR(1) NOT NULL, 
  InstructionnalDayType_MakeUpDay CHAR(1) NOT NULL, 
  BlockScheduleDay NVARCHAR(50) NOT NULL, -- A-Day, B-day
  BlockScheduleDay_ADay CHAR(1) NOT NULL, -- Y, N
  BlockScheduleDay_BDay CHAR(1) NOT NULL, -- Y, N
  Semester INT NULL, -- 1,2
  SemesterCode NVARCHAR(50) NULL, -- S1,S1
  SemesterDescription NVARCHAR(50) NULL, -- Semester 1, Semester 2
  Trimester INT NULL, -- 1,2,3
  TrimesterCode NVARCHAR(50) NULL, -- T1,T2
  TrimesterDescription NVARCHAR(50) NULL, -- Timester 1, Timester 2, Timester 3
  [Quarter] INT NULL, -- 1,2,3,4
  QuarterCode NVARCHAR(50) NULL, -- Q1,Q2,Q3,Q4
  QuarterDescription NVARCHAR(50) NULL, -- Quarter 1, Quarter 2, Quarter 3, Quarter 4



  [IsWeekend] BIT NOT NULL,
  [IsHoliday] BIT NOT NULL, --  1,0
  [HolidayName] NVARCHAR(20) NOT NULL, 
  [SpecialDay] NVARCHAR(20) NOT NULL, --  Valentine Day


  WeekBeforeChristmasIndicator CHAR(1) NOT NULL, --  Y, N

  StateExaminationPeriodIdicator CHAR(1) NOT NULL, --  Y,N

  SchoolYear INT NOT NULL, -- ex: 9/1/2019 = 2020
  SchoolYearDescription NVARCHAR(50) NOT NULL, -- '2019-2020 or SchoolYear 2019 - 2020'
  
  [Month] TINYINT NOT NULL, -- 1..12
  [Month Name]  NVARCHAR(10) NOT NULL, --January,February,December
  [Month Name Short]  CHAR(3) NOT NULL, --Jan,Feb,Dec
  [Month Name First Letter]  CHAR(1) NOT NULL, --J,F,D

  [Day Of Month]  int NOT NULL, -- 1 - 30|31
  [Day Of Week]  int NOT NULL, -- 1 -7
  [Day Name]  NVARCHAR(15) NOT NULL, -- Monday, Tuesday
   
  CalendarYear INT NOT NULL, -- ex: 9/1/2019 = 2019

 
  

  EffectiveStartDate DATETIME NOT NULL,
  EffectiveEndDate DATETIME NOT NULL,
  IsCurrent BIT NOT NULL,
  --ETLProcessedDateTime DATETIME NULL,
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimTime PRIMARY KEY (TimeKey),
  CONSTRAINT FK_DimTime_AcademicCalendarKey FOREIGN KEY (AcademicCalendarKey) REFERENCES dbo.DimAcademicCalendar(AcademicCalendarKey),
  CONSTRAINT FK_DimTime_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES [dbo].[DimSchool] (SchoolKey),
  CONSTRAINT FK_DimTime_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);


CREATE TABLE dbo.DimStudent
(
  StudentKey INT NOT NULL IDENTITY(1,1),  -- surrogate
  [_sourceKey] NVARCHAR(50) NOT NULL,

  DistrictStudentId NVARCHAR(100) NULL,
  StateStudentId NVARCHAR(100) NULL,
  
  SchoolKey INT NOT NULL,
  SchoolCode NVARCHAR(10) NOT NULL,
  SchoolName NVARCHAR(100) NOT NULL,
  GradeLevelCode CHAR(2) NOT NULL,
  GradeLevelDescription CHAR(2) NOT NULL,
  
  FirstName NVARCHAR(50) NOT NULL,
  MiddleInitial CHAR(1) NULL,
  MiddleName NVARCHAR(50) NULL,
  LastName NVARCHAR(50) NOT NULL,
  FullName NVARCHAR(50) NOT NULL,
  DateOfBirth DATE NOT NULL,
  CurrentAge INT NOT NULL,
  SchoolYearAdmitted INT NOT NULL,
  GradautedIndicator CHAR(1) NULL,
  
  Homeroom  NVARCHAR(50) NULL,
  HomeroomTeacher NVARCHAR(100) NULL,

  GenderCode NVARCHAR(15) NOT NULL,
  Gender_Description NVARCHAR(100) NOT NULL,    
  Gender_MaleIndicator CHAR(1) NOT NULL,
  Gender_FemaleIndicator CHAR(1) NOT NULL,
  
  RaceCode NVARCHAR(15) NOT NULL,
  RaceDescription NVARCHAR(100) NOT NULL,  
  
  Race_AmericanIndianAlaskanNativeIndicator CHAR(1) NOT NULL,
  Race_AsianIndicator CHAR(1) NOT NULL,
  Race_BlackAfricaAmericanIndicator CHAR(1) NOT NULL,
  Race_NativeHawaiianPacificIslanderIndicator CHAR(1) NOT NULL,
  Race_WhiteIndicator CHAR(1) NOT NULL,
  Race_MultiRaceIndicator CHAR(1) NOT NULL,
  Race_ChooseNotRespondIndicator CHAR(1) NOT NULL,
  Race_OtherIndicator CHAR(1) NOT NULL,
	
  EthnicityCode NVARCHAR(15) NOT NULL,
  EthnicityDescription NVARCHAR(100) NOT NULL,
  EthnicityHispanicLatinoIndicator CHAR(1) NOT NULL,
    
  IEPIndicator CHAR(1) NOT NULL,
  
  ELLStatusCode NVARCHAR(1) NOT NULL, -- L, F, N 
  ELLStatusDescription NVARCHAR(25) NOT NULL, -- English Learner , Former, Neither  
  ELL_EnglishLearnerIndicator CHAR(1) NOT NULL,
  ELL_FormerIndicator CHAR(1) NOT NULL,
  ELL_NotEnglisLearnerIndicator CHAR(1) NOT NULL,
  
  EconomicDisadvantageIndicator CHAR(1) NOT NULL,   

  EnrollmentEntryDate DATETIME2 NOT NULL,
  EnrollmentEntryCode NVARCHAR(25) NOT NULL, 
  EnrollmentExitDate  DATETIME2 NULL,
  EnrollmentExitCode  NVARCHAR(25) NULL,
     
  EffectiveStartDate DATETIME NOT NULL, 
  EffectiveEndDate DATETIME NOT NULL, 
  IsCurrent BIT NOT NULL,
  --ETLProcessedDateTime DATETIME NULL,
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimStudent PRIMARY KEY (StudentKey),
  CONSTRAINT FK_DimStudent_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES dbo.DimSchool(SchoolKey),
  CONSTRAINT FK_DimStudent_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);


CREATE TABLE dbo.DimAssessment
(
    AssessmentKey INT NOT NULL IDENTITY(1,1),  
	[_sourceKey] NVARCHAR(50) NOT NULL,
	SubjectArea NVARCHAR(50) NOT NULL,    
	SubjectCode NVARCHAR(25) NOT NULL,    

	TestAbbreviation NVARCHAR(25) NOT NULL,   
	TestShortName NVARCHAR(100) NOT NULL,
	TestDescripton NVARCHAR(500) NOT NULL,
	
	TestPartAbbreviation NVARCHAR(25) NOT NULL,    
	TestPartShortName NVARCHAR(100) NOT NULL,
	TestPartDescripton NVARCHAR(500) NOT NULL,
	
	TestSubPartAbbreviation NVARCHAR(25) NOT NULL,    
	TestSubPartShortName NVARCHAR(100) NOT NULL,
	TestSubPartDescripton NVARCHAR(500) NOT NULL,

	TestSubPartDataType NVARCHAR(50) NOT NULL, -- Integer, Decimmal, Alphanumeric, Date, Datetime, etc
	TestSubPartDataType_IntegerIdicator bit NOT NULL, 
	TestSubPartDataType_DecimalIdicator bit NOT NULL, 
	TestSubPartDataType_AlphanumericIdicator bit NOT NULL, 
	TestSubPartDataType_DateIdicator bit NOT NULL, 
	TestSubPartDataType_DatetimeIndicator bit NOT NULL, 
	
	EffectiveStartDate DATETIME NOT NULL, 
	EffectiveEndDate DATETIME NOT NULL, 
	IsCurrent BIT NOT NULL,
	--ETLProcessedDateTime DATETIME NULL,
    [LineageKey] INT NOT NULL,

	CONSTRAINT PK_DimAssessment PRIMARY KEY (AssessmentKey),
    CONSTRAINT FK_DimAssessment_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);



CREATE TABLE dbo.DimCourse
(
    CourseKey INT NOT NULL IDENTITY(1,1),  
	[_sourceKey] NVARCHAR(50) NOT NULL,
	
	CourseCode NVARCHAR(15) NOT NULL,
	CourseTitle NVARCHAR(100) NOT NULL,
	CourseDescription NVARCHAR(100) NOT NULL,

	SubjectArea NVARCHAR(50) NOT NULL,    
	SubjectCode NVARCHAR(25) NOT NULL,    

	CourseType NVARCHAR(25) NOT NULL,    -- Dual Enrolment, Dual Credit, AP, IB
	CourseType_DualCreditIndicator CHAR(1) NOT NULL,    -- Y,N
	CourseType_APIndicator CHAR(1) NOT NULL,    -- Y,N
	CourseType_HighSchoolIndicator CHAR(1) NOT NULL,    -- Y,N
		
	EffectiveStartDate DATETIME NOT NULL, 
	EffectiveEndDate DATETIME NOT NULL, 
	IsCurrent BIT NOT NULL,
	--ETLProcessedDateTime DATETIME NULL,
    [LineageKey] INT NOT NULL,

  	CONSTRAINT PK_DimCourse PRIMARY KEY (CourseKey),
    CONSTRAINT FK_DimCourse_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);

/*
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

  --ETLProcessedDateTime DATETIME NULL,
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

  --ETLProcessedDateTime DATETIME NULL,
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

  ScoreValue NVARCHAR(20) NOT NULL,

  Subject NVARCHAR(25) NOT NULL,
  RawScore INT NULL,
  ScaleScore INT NULL,
  PerformanceLevel INT NULL,
  StudentGrowthPercentile INT NULL,
  --ETLProcessedDateTime DATETIME NULL,
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

  --ETLProcessedDateTime DATETIME NULL,
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_FactStudentCourseGrade PRIMARY KEY (StudentKey ASC, TimeKey ASC, CourseKey ASC),
  CONSTRAINT FK_FactStudentCourseGrade_StudentKey FOREIGN KEY (StudentKey) REFERENCES dbo.DimStudent(StudentKey),
  CONSTRAINT FK_FactStudentCourseGrade_TimeKey FOREIGN KEY (TimeKey) REFERENCES dbo.DimTime(TimeKey),
  CONSTRAINT FK_FactStudentCourseGrade_CourseKey FOREIGN KEY (CourseKey) REFERENCES dbo.DimCourse(CourseKey) ,
  CONSTRAINT FK_FactStudentCourseGrade_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);



