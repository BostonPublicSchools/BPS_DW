/*

--section
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimSection' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.DimSection
(
  SectionKey INT NOT NULL IDENTITY(1,1),
  [_sourceKey] NVARCHAR(50) NOT NULL,  --'Ed-Fi|25590100101Trad120ENG112011'
  
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

--staff
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimStaff' 
			   AND TABLE_SCHEMA = 'dbo')
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

--discipline incident types
-- NOT USED FOR NOW
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimDisciplineIncidentBehavior' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.DimDisciplineIncidentBehavior
(
  DisciplineIncidentBehaviorKey INT NOT NULL IDENTITY(1,1),
  [_sourceKey] NVARCHAR(50) NOT NULL, 
  
  [BehaviorDescriptor_CodeValue] nvarchar(50) not null,
  [BehaviorDescriptor_Description] nvarchar(1024) not null,
  
  ValidFrom DATETIME NOT NULL, 
  ValidTo DATETIME NOT NULL, 
  IsCurrent BIT NOT NULL,    
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimDisciplineIncidentBehavior PRIMARY KEY (DisciplineIncidentBehaviorKey ASC)  
);

--discipline incident location
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimDisciplineIncidentLocation' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.DimDisciplineIncidentLocation
(
  DisciplineIncidentLocationKey INT NOT NULL IDENTITY(1,1),
  [_sourceKey] NVARCHAR(50) NOT NULL, 
  
  [LocationDescriptor_CodeValue] nvarchar(50) not null,
  [LocationDescriptor_Description] nvarchar(1024) not null,
  
  ValidFrom DATETIME NOT NULL, 
  ValidTo DATETIME NOT NULL, 
  IsCurrent BIT NOT NULL,    
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimDisciplineIncidentLocation PRIMARY KEY (DisciplineIncidentLocationKey  ASC)     
);

--discipline incident location
-- NOT USED FOR NOW
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimDisciplineIncidentAction' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.DimDisciplineIncidentAction
(
  DisciplineIncidentActionKey INT NOT NULL IDENTITY(1,1),
  [_sourceKey] NVARCHAR(50) NOT NULL, 
  
  [DisciplineDescriptor_CodeValue] nvarchar(50) not null,
  [DisciplineDescriptor_Description] nvarchar(1024) not null,
  DisciplineDescriptor_ISS_Indicator BIT NOT NULL,
  DisciplineDescriptor_OSS_Indicator BIT NOT NULL,
  ValidFrom DATETIME NOT NULL, 
  ValidTo DATETIME NOT NULL, 
  IsCurrent BIT NOT NULL,    
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimDisciplineIncidentAction PRIMARY KEY (DisciplineIncidentActionKey ASC)  
);

--discipline incident reporter types
-- NOT USED FOR NOW
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimDisciplineIncidentReporterType' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.DimDisciplineIncidentReporterType
(
  DisciplineIncidentReporterTypeKey INT NOT NULL IDENTITY(1,1),
  [_sourceKey] NVARCHAR(50) NOT NULL, 
  
  ReporterDescriptor_CodeValue nvarchar(50) NOT NULL,
  ReporterDescriptor_Description nvarchar(1024) NOT NULL,
  
  ValidFrom DATETIME NOT NULL, 
  ValidTo DATETIME NOT NULL, 
  IsCurrent BIT NOT NULL,    
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimDisciplineIncidentReporterType PRIMARY KEY (DisciplineIncidentReporterTypeKey ASC)  
);

--attendance by section
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'FactStudentAttendanceBySection' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.FactStudentAttendanceBySection
(
  StudentKey INT NOT NULL,
  TimeKey INT NOT NULL,  
  SectionKey INT NOT NULL,
  StaffKey INT NOT NULL,
  SchoolKey INT NOT NULL,

  AttendanceEventCategoryKey INT NOT NULL ,
  AttendanceEventReason nvarchar(500) NOT NULL,

  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_FactStudentAttendanceBySection PRIMARY KEY (StudentKey ASC, TimeKey ASC, SectionKey ASC,SchoolKey ASC, StaffKey ASC, AttendanceEventCategoryKey ASC),
  CONSTRAINT FK_FactStudentAttendanceBySection_StudentKey FOREIGN KEY (StudentKey) REFERENCES dbo.DimStudent(StudentKey),
  CONSTRAINT FK_FactStudentAttendanceBySection_TimeKey FOREIGN KEY (TimeKey) REFERENCES dbo.DimTime(TimeKey),
  CONSTRAINT FK_FactStudentAttendanceBySection_SectionKey FOREIGN KEY (SectionKey) REFERENCES dbo.DimSection(SectionKey),
  CONSTRAINT FK_FactStudentAttendanceBySection_StaffKey FOREIGN KEY (StaffKey) REFERENCES dbo.DimStaff(StaffKey),
  CONSTRAINT FK_FactStudentAttendanceBySection_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES dbo.DimSchool(SchoolKey),
  CONSTRAINT FK_FactStudentAttendanceBySection_AttendanceEventCategoryKey FOREIGN KEY (AttendanceEventCategoryKey) REFERENCES dbo.DimAttendanceEventCategory(AttendanceEventCategoryKey),
  CONSTRAINT FK_FactStudentAttendanceBySection_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.ETL_Lineage([LineageKey])  
);


--discipline -- v1

if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'FactStudentDiscipline' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.FactStudentDiscipline
(
  StudentKey INT NOT NULL,
  TimeKey INT NOT NULL, 
  SchoolKey INT NOT NULL,
  IncidentTime TIME(7) NOT NULL, 
  
  DisciplineIncidentBehaviorKey INT NOT NULL, -- Weapons Possession (Firearms and Other Weapons), Drugs. etcs
  DisciplineIncidentLocationKey INT NOT NULL, -- Hallway, Cafeteria, Classroom, etc
  DisciplineIncidentActionKey INT NOT NULL, -- ISS, OSS
  DisciplineIncidentReporterTypeKey INT NOT NULL, --Law enforcement officer,Non-school personnel,Other,Parent/guardian,Staff,Student,


  IncidentReporterName NVARCHAR(100) NULL ,
  ReportedToLawEnforcement_Indicator BIT NOT NULL ,
  IncidentCost Money NOT NULL,
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_FactStudentDiscipline PRIMARY KEY (StudentKey ASC, TimeKey ASC, SchoolKey ASC, IncidentTime ASC),
  CONSTRAINT FK_FactStudentDiscipline_StudentKey FOREIGN KEY (StudentKey) REFERENCES dbo.DimStudent(StudentKey),
  CONSTRAINT FK_FactStudentDiscipline_TimeKey FOREIGN KEY (TimeKey) REFERENCES dbo.DimTime(TimeKey),
  CONSTRAINT FK_FactStudentDiscipline_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES dbo.DimSchool(SchoolKey),
  CONSTRAINT FK_FactStudentDiscipline_DisciplineIncidentBehaviorKey FOREIGN KEY (DisciplineIncidentBehaviorKey) REFERENCES dbo.DimDisciplineIncidentBehavior(DisciplineIncidentBehaviorKey),
  CONSTRAINT FK_FactStudentDiscipline_DisciplineIncidentLocationKey FOREIGN KEY (DisciplineIncidentLocationKey) REFERENCES dbo.DimDisciplineIncidentLocation(DisciplineIncidentLocationKey),
  CONSTRAINT FK_FactStudentDiscipline_DisciplineIncidentActionKey FOREIGN KEY (DisciplineIncidentActionKey) REFERENCES dbo.DimDisciplineIncidentAction(DisciplineIncidentActionKey),
  CONSTRAINT FK_FactStudentDiscipline_DisciplineIncidentReporterTypeKey FOREIGN KEY (DisciplineIncidentReporterTypeKey) REFERENCES dbo.DimDisciplineIncidentReporterType(DisciplineIncidentReporterTypeKey),
  
  CONSTRAINT FK_FactStudentDiscipline_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.ETL_Lineage([LineageKey])
);
*/
