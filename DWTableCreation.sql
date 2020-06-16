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

--DIMENSION TABLES
-------------------------------------------------------------------------------------------------
CREATE TABLE dbo.DimSchool
(
  SchoolKey int NOT NULL IDENTITY(1,1), -- ex 9/1/2019 : 20190901 -- surrogate
  [_sourceKey] NVARCHAR(50) NOT NULL,  --'ODS|Id'
  
  ShortNameOfInstitution NVARCHAR(500) NOT NULL,     
  NameOfInstitution NVARCHAR(500) NOT NULL,    

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
    
  DayOfYear SMALLINT NULL, -- 1 - 365 or 366 (Leap Year Every Four Years)  
  
  LeapYear_Indicator BIT NOT NULL,  
    
  FederalHolidayName NVARCHAR(20) NULL, -- Memorial Day
  FederalHoliday_Indicator BIT NOT NULL, --  True,False
  
  --all these vary by school
  SchoolKey INT NULL,  
  DayOfSchoolYear SMALLINT NULL, -- 1 - 180 - based on SIS(ODS) school calendar
  SchoolCalendarEventType_CodeValue NVARCHAR(50) NULL, -- Emergency day,Instructional day,Teacher only day
  SchoolCalendarEventType_Description NVARCHAR(50) NULL, -- Emergency day,Instructional day,Teacher only day
    
  SchoolTermDescriptor_CodeValue NVARCHAR(50) NULL, -- Year Round,First Quarter, First Trimester, Fall Semester, Fourth Quarter, etc.  SELECT * FROM v25_EdFi_Ods_Sandbox_populatedSandbox.edfi.Descriptor where namespace = 'http://ed-fi.org/Descriptor/TermDescriptor.xml'
  SchoolTermDescriptor_Description NVARCHAR(50) NULL, -- Year Round,First Quarter, First Trimester, Fall Semester, Fourth Quarter, etc SELECT * FROM v25_EdFi_Ods_Sandbox_populatedSandbox.edfi.Descriptor where namespace = 'http://ed-fi.org/Descriptor/TermDescriptor.xml'

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
    [StudentKey] [int] IDENTITY(1,1) NOT NULL, 
	[_sourceKey] [nvarchar](50) NOT NULL,

	[StudentUniqueId] [nvarchar](32) NULL,
	[StateId] [nvarchar](32) NULL,

	PrimaryElectronicMailAddress [nvarchar](128) NULL,
	PrimaryElectronicMailTypeDescriptor_CodeValue [nvarchar](128) NULL, -- Home/Personal, Organization, Other, Work
	PrimaryElectronicMailTypeDescriptor_Description [nvarchar](128) NULL,

	[SchoolKey] [int] NOT NULL,
	[ShortNameOfInstitution] [nvarchar](500) NOT NULL,
	[NameOfInstitution] [nvarchar](500) NOT NULL,
	[GradeLevelDescriptor_CodeValue] [nvarchar](100) NOT NULL,
	[GradeLevelDescriptor_Description] [nvarchar](500) NOT NULL,
	
	[FirstName] [nvarchar](100) NOT NULL,
	[MiddleInitial] [char](1) NULL,
	[MiddleName] [nvarchar](100) NULL,
	[LastSurname] [nvarchar](100) NOT NULL,
	[FullName] [nvarchar](500) NOT NULL,
	[BirthDate] [date] NOT NULL,
	[StudentAge] [int] NOT NULL,
	[GraduationSchoolYear] [int] NULL,
	
	[Homeroom] [nvarchar](500) NULL,
	[HomeroomTeacher] [nvarchar](500) NULL,

	[SexType_Code] [nvarchar](100) NOT NULL,
	[SexType_Description] [nvarchar](100) NOT NULL,
	[SexType_Male_Indicator] [bit] NOT NULL,
	[SexType_Female_Indicator] [bit] NOT NULL,
	[SexType_NotSelected_Indicator] [bit] NOT NULL,
	
	[RaceCode] [nvarchar](100) NOT NULL,
	[RaceDescription] [nvarchar](100) NOT NULL,
	[Race_AmericanIndianAlaskanNative_Indicator] [bit] NOT NULL,
	[Race_Asian_Indicator] [bit] NOT NULL,
	[Race_BlackAfricaAmerican_Indicator] [bit] NOT NULL,
	[Race_NativeHawaiianPacificIslander_Indicator] [bit] NOT NULL,
	[Race_White_Indicator] [bit] NOT NULL,
	[Race_MultiRace_Indicator] [bit] NOT NULL,
	[Race_ChooseNotRespond_Indicator] [bit] NOT NULL,
	[Race_Other_Indicator] [bit] NOT NULL,

	[EthnicityCode] [nvarchar](100) NOT NULL,
	[EthnicityDescription] [nvarchar](100) NOT NULL,
	[EthnicityHispanicLatino_Indicator] [bit] NOT NULL,
	[Migrant_Indicator] [bit] NOT NULL,
	[Homeless_Indicator] [bit] NOT NULL,
	[IEP_Indicator] [bit] NOT NULL,
	[LimitedEnglishProficiencyDescriptor_CodeValue] [nvarchar](100) NOT NULL,
	[LimitedEnglishProficiencyDescriptor_Description] [nvarchar](100) NOT NULL,
	[LimitedEnglishProficiency_EnglishLearner_Indicator] [bit] NOT NULL,
	[LimitedEnglishProficiency_Former_Indicator] [bit] NOT NULL,
	[LimitedEnglishProficiency_NotEnglisLearner_Indicator] [bit] NOT NULL,
	[EconomicDisadvantage_Indicator] [bit] NOT NULL,
	
	[EntryDate] [datetime2](7) NOT NULL,
	[EntrySchoolYear] [int] NOT NULL,
	[EntryCode] [nvarchar](25) NOT NULL,
	
	[ExitWithdrawDate] [datetime2](7) NULL,
	[ExitWithdrawSchoolYear] [int] NULL,
	[ExitWithdrawCode] [nvarchar](100) NULL,

	[ValidFrom] [datetime] NOT NULL,
	[ValidTo] [datetime] NOT NULL,
	[IsCurrent] [bit] NOT NULL,
	[LineageKey] [int] NOT NULL,

    CONSTRAINT PK_DimStudent PRIMARY KEY (StudentKey),
    CONSTRAINT FK_DimStudent_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES dbo.DimSchool(SchoolKey),
    CONSTRAINT FK_DimStudent_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);

CREATE TABLE dbo.DimSection
(
  SectionKey INT NOT NULL IDENTITY(1,1),
  [_sourceKey] NVARCHAR(50) NOT NULL,  --'ODS|25590100101Trad120ENG112011'
  
  [SchoolYear] [smallint] NOT NULL,  
  SchoolKey INT NOT NULL,
  ShortNameOfInstitution nvarchar(500) NOT NULL,
  NameOfInstitution nvarchar(500) NOT NULL,
  [ClassPeriodName] [nvarchar](20) NOT NULL,
  [ClassroomIdentificationCode] [nvarchar](20) NOT NULL,
  [LocalCourseCode] [nvarchar](60) NOT NULL,
  [SchoolTermDescriptor_CodeValue] nvarchar(50) not null,
  [SchoolTermDescriptor_Description] nvarchar(50) not null,
  
  ValidFrom DATETIME NOT NULL, 
  ValidTo DATETIME NOT NULL, 
  IsCurrent BIT NOT NULL,    
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimSection PRIMARY KEY (SectionKey ASC),
  CONSTRAINT FK_DimSection_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES dbo.DimSchool(SchoolKey)
);


CREATE TABLE dbo.DimStaff
(
  StaffKey INT NOT NULL IDENTITY(1,1),
  [_sourceKey] NVARCHAR(50) NOT NULL,  --'EdFi|StaffUSI'
  
  PrimaryElectronicMailAddress [nvarchar](128) NULL,
  PrimaryElectronicMailTypeDescriptor_CodeValue [nvarchar](128) NULL, -- Home/Personal, Organization, Other, Work
  PrimaryElectronicMailTypeDescriptor_Description [nvarchar](128) NULL,

  [StaffUniqueId] [nvarchar](32) NOT NULL,
  [PersonalTitlePrefix] [nvarchar](30) NULL,
  [FirstName] [nvarchar](75) NOT NULL,
  [MiddleName] [nvarchar](75) NULL,
  [MiddleInitial] CHAR(1) NULL,
  [LastSurname] nvarchar(75) NOT NULL,
  [FullName] NVARCHAR(50) NOT NULL,
  [GenerationCodeSuffix] [nvarchar](10) NULL,
  [MaidenName] [nvarchar](75) NULL,  
  [BirthDate] DATE NULL,
  [StaffAge] INT NULL,  
  
  SexType_Code NVARCHAR(15) NOT NULL,
  SexType_Description NVARCHAR(100) NOT NULL,    
  SexType_Male_Indicator BIT NOT NULL,
  SexType_Female_Indicator BIT NOT NULL,
  SexType_NotSelected_Indicator BIT NOT NULL,
  
  [HispanicLatinoEthnicity_Indicator] [bit] NOT NULL,
  [OldEthnicityType_CodeValue] NVARCHAR(100)  NULL,
  [OldEthnicityType_Description] NVARCHAR(100)  NULL,
  [CitizenshipStatusType_CodeValue] NVARCHAR(100)  NULL,
  [CitizenshipStatusType_Description] NVARCHAR(100)  NULL,
  [HighestLevelOfEducationDescriptorDescriptor_CodeValue] NVARCHAR(100)  NULL, 
  [HighestLevelOfEducationDescriptorDescriptor_Description] NVARCHAR(100)  NULL, 
  [YearsOfPriorProfessionalExperience] [decimal](5, 2) NULL,
  [YearsOfPriorTeachingExperience] [decimal](5, 2) NULL,  
  [HighlyQualifiedTeacher_Indicator] [bit] NULL,
    
  ValidFrom DATETIME NOT NULL, 
  ValidTo DATETIME NOT NULL, 
  IsCurrent BIT NOT NULL,    
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimStaff PRIMARY KEY (StaffKey)  
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

	AcademicSubjectDescriptor_Biology_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_CareerAndTechnicalEducation_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_Chemistry_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_Composite_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_CriticalReading_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_EnglishLanguageArts_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_English_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_FineAndPerformingArts_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_ForeignLanguageAndLiterature_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_IntroductoryPhysics_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_LifeAndPhysicalSciences_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_Mathematics_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_MilitaryScience_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_Other_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_PhysicalHealthAndSafetyEducation_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_Reading_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_ReligiousEducationAndTheology_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_Science_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_SocialSciencesAndHistory_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_SocialStudies_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_TechnologyEngineering_Indicator BIT NOT NULL,
	AcademicSubjectDescriptor_Writing_Indicator BIT NOT NULL,

	HighSchoolCourseRequirement_Indicator BIT NOT NULL,
	MinimumAvailableCredits INT NULL,
	MaximumAvailableCredits INT NULL,

	GPAApplicabilityType_CodeValue NVARCHAR(50) NULL,
	GPAApplicabilityType_Description NVARCHAR(50) NULL,
			
	ValidFrom DATETIME NOT NULL, 
	ValidTo DATETIME NOT NULL, 
	IsCurrent BIT NOT NULL,	
    [LineageKey] INT NOT NULL,

  	CONSTRAINT PK_DimCourse PRIMARY KEY (CourseKey),
    CONSTRAINT FK_DimCourse_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);

--FACT TABLES
----------------------------------------------------------------------

CREATE TABLE dbo.FactStudentBehavior
(
  StudentKey INT NOT NULL,
  TimeKey INT NOT NULL, 
  IncidentDateTime DATETIME NOT NULL, 
  IncidentTypeCode NVARCHAR(100) NOT NULL,
  IncidentActionCode NVARCHAR(100) NULL,
  IncidentLocationCode NVARCHAR(500) NULL,  
  [ISS_Indicator] BIT NOT NULL,  
  [OSS_Indicator] BIT NOT NULL,
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_FactStudentBehavior PRIMARY KEY (StudentKey ASC, TimeKey ASC, IncidentDateTime ASC),
  CONSTRAINT FK_FactStudentBehavior_StudentKey FOREIGN KEY (StudentKey) REFERENCES dbo.DimStudent(StudentKey),
  CONSTRAINT FK_FactStudentBehavior_TimeKey FOREIGN KEY (TimeKey) REFERENCES dbo.DimTime(TimeKey),
  CONSTRAINT FK_FactStudentBehavior_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);

CREATE TABLE dbo.FactStudentAttendanceByDay
(
  StudentKey INT NOT NULL,
  TimeKey INT NOT NULL,  
  SchoolKey INT NOT NULL,
  
  AttendanceEventCategoryDescriptor_CodeValue nvarchar(50) NOT NULL,
  AttendanceEventCategoryDescriptor_Description nvarchar(50) NOT NULL,
  AttendanceEventReason nvarchar(500) NOT NULL,
  [InAttendance_Indicator] BIT NOT NULL,  
  [UnexcusedAbsence_Indicator] BIT NOT NULL,
  [ExcusedAbsence_Indicator] BIT NOT NULL,  
  [Tardy_Indicator] BIT NOT NULL,    
 -- [UnexcusedTardy_Indicator] BIT NOT NULL,  
  --[ExcusedTardy_Indicator] BIT NOT NULL,  
  [EarlyDeparture_Indicator]  BIT NOT NULL,  
  --[NoContact_Indicator] BIT NOT NULL,    
  --[ADA_Indicator] INT NOT NULL,    
  
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_FactStudentAttendance PRIMARY KEY (StudentKey ASC, TimeKey ASC),
  CONSTRAINT FK_FactStudentAttendance_StudentKey FOREIGN KEY (StudentKey) REFERENCES dbo.DimStudent(StudentKey),
  CONSTRAINT FK_FactStudentAttendance_TimeKey FOREIGN KEY (TimeKey) REFERENCES dbo.DimTime(TimeKey),
  CONSTRAINT FK_FactStudentAttendance_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES dbo.DimSchool(SchoolKey),
  CONSTRAINT FK_FactStudentAttendance_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
  
);

CREATE TABLE dbo.FactStudentAttendanceBySection
(
  StudentKey INT NOT NULL,
  TimeKey INT NOT NULL,  
  SectionKey INT NOT NULL,
  StaffKey INT NOT NULL,
  SchoolKey INT NOT NULL,

  AttendanceEventCategoryDescriptor_CodeValue nvarchar(50) NOT NULL,
  AttendanceEventCategoryDescriptor_Description nvarchar(50) NOT NULL,
  AttendanceEventReason nvarchar(500) NOT NULL,
  [InAttendance_Indicator] BIT NOT NULL,  
  [UnexcusedAbsence_Indicator] BIT NOT NULL,
  [ExcusedAbsence_Indicator] BIT NOT NULL,  
  [Tardy_Indicator] BIT NOT NULL,    
  --[UnexcusedTardy_Indicator] BIT NOT NULL,  
  --[ExcusedTardy_Indicator] BIT NOT NULL,  
  [EarlyDeparture_Indicator]  BIT NOT NULL,  
  --[NoContact_Indicator] BIT NOT NULL,    
  [ADA_Indicator] INT NOT NULL,  
  
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_FactStudentAttendanceBySection PRIMARY KEY (StudentKey ASC, TimeKey ASC, SectionKey ASC, StaffKey),
  CONSTRAINT FK_FactStudentAttendanceBySection_StudentKey FOREIGN KEY (StudentKey) REFERENCES dbo.DimStudent(StudentKey),
  CONSTRAINT FK_FactStudentAttendanceBySection_TimeKey FOREIGN KEY (TimeKey) REFERENCES dbo.DimTime(TimeKey),
  CONSTRAINT FK_FactStudentAttendanceBySection_SectionKey FOREIGN KEY (SectionKey) REFERENCES dbo.DimSection(SectionKey),
  CONSTRAINT FK_FactStudentAttendanceBySection_StaffKey FOREIGN KEY (StaffKey) REFERENCES dbo.DimStaff(StaffKey),
  CONSTRAINT FK_FactStudentAttendanceBySection_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES dbo.DimSchool(SchoolKey),
  CONSTRAINT FK_FactStudentAttendanceBySection_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])  
);




CREATE TABLE dbo.FactStudentAssessmentScore
(
  StudentKey INT NOT NULL,
  TimeKey INT NOT NULL, 
  AssessmentKey INT NOT NULL,
  
   
  Score_AssessmentReportingMethodDescriptor_RawScore INT NULL,
  Score_AssessmentReportingMethodDescriptor_ScaleScore INT NULL,
  Score_AssessmentReportingMethodDescriptor_ProficiencyLevel  NVARCHAR(25) NULL,
  Score_AssessmentReportingMethodDescriptor_Percentile FLOAT NULL,
  
  MCAS_PerformanceLevel_Descriptor_Failing_Indicator BIT NOT NULL , -- grade 10 tests
  MCAS_PerformanceLevel_Descriptor_Warning_Indicator BIT NOT NULL , -- grade 3-8 tests
  MCAS_PerformanceLevel_Descriptor_NeedsImprovement_Indicator BIT NOT NULL ,  
  MCAS_PerformanceLevel_Descriptor_Proficient_Indicator BIT NOT NULL ,
  MCAS_PerformanceLevel_Descriptor_Advanced_Indicator BIT NOT NULL ,
        
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



