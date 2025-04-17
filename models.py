from datetime import datetime

class SampleData:
    @staticmethod
    def get_students_data():
        return [
            {
                "StudentID": 1001,
                "FirstName": "John",
                "LastName": "Smith",
                "DateOfBirth": datetime(1998, 5, 15),
                "Gender": "M",
                "Email": "john.smith@university.edu",
                "Phone": "555-123-4567",
                "Address": "123 Main St, Anytown, USA",
                "EnrollmentDate": datetime(2017, 8, 20),
                "Major": "Computer Science",
                "GPA": 3.75
            },
            {
                "StudentID": 1002,
                "FirstName": "Emily",
                "LastName": "Johnson",
                "DateOfBirth": datetime(1999, 2, 28),
                "Gender": "F",
                "Email": "emily.johnson@university.edu",
                "Phone": "555-234-5678",
                "Address": "456 Oak Ave, Somewhere, USA",
                "EnrollmentDate": datetime(2018, 1, 15),
                "Major": "Biology",
                "GPA": 3.92
            },
            {
                "StudentID": 1003,
                "FirstName": "Michael",
                "LastName": "Williams",
                "DateOfBirth": datetime(1997, 11, 5),
                "Gender": "M",
                "Email": "michael.williams@university.edu",
                "Phone": "555-345-6789",
                "Address": "789 Pine Rd, Nowhere, USA",
                "EnrollmentDate": datetime(2016, 9, 1),
                "Major": "Electrical Engineering",
                "GPA": 3.45
            },
            {
                "StudentID": 1004,
                "FirstName": "Sophia",
                "LastName": "Lee",
                "DateOfBirth": datetime(2000, 4, 12),
                "Gender": "F",
                "Email": "sophia.lee@university.edu",
                "Phone": "555-456-7890",
                "Address": "321 Elm St, Uptown, USA",
                "EnrollmentDate": datetime(2019, 9, 1),
                "Major": "Mathematics",
                "GPA": 3.88
            },
            {
                "StudentID": 1005,
                "FirstName": "David",
                "LastName": "Brown",
                "DateOfBirth": datetime(1998, 8, 30),
                "Gender": "M",
                "Email": "david.brown@university.edu",
                "Phone": "555-567-8901",
                "Address": "654 Cedar St, Hometown, USA",
                "EnrollmentDate": datetime(2017, 1, 10),
                "Major": "Mechanical Engineering",
                "GPA": 3.60
            },
            {
                "StudentID": 1006,
                "FirstName": "Olivia",
                "LastName": "Garcia",
                "DateOfBirth": datetime(1999, 7, 22),
                "Gender": "F",
                "Email": "olivia.garcia@university.edu",
                "Phone": "555-678-9012",
                "Address": "987 Maple Dr, Lakeside, USA",
                "EnrollmentDate": datetime(2018, 8, 25),
                "Major": "Psychology",
                "GPA": 3.78
            },
            {
                "StudentID": 1007,
                "FirstName": "Daniel",
                "LastName": "Martinez",
                "DateOfBirth": datetime(1997, 3, 18),
                "Gender": "M",
                "Email": "daniel.martinez@university.edu",
                "Phone": "555-789-0123",
                "Address": "159 Birch Ln, Mountainview, USA",
                "EnrollmentDate": datetime(2016, 1, 15),
                "Major": "Business Administration",
                "GPA": 3.65
            },
            {
                "StudentID": 1008,
                "FirstName": "Emma",
                "LastName": "Davis",
                "DateOfBirth": datetime(2000, 9, 5),
                "Gender": "F",
                "Email": "emma.davis@university.edu",
                "Phone": "555-890-1234",
                "Address": "753 Willow Way, Riverside, USA",
                "EnrollmentDate": datetime(2019, 8, 20),
                "Major": "Chemistry",
                "GPA": 3.95
            }
        ]

    @staticmethod
    def get_courses_data():
        return [
            {
                "CourseID": 101,
                "CourseCode": "CS101",
                "CourseName": "Introduction to Computer Science",
                "CreditHours": 4,
                "Department": "Computer Science"
            },
            {
                "CourseID": 102,
                "CourseCode": "BIO201",
                "CourseName": "Cell Biology",
                "CreditHours": 3,
                "Department": "Biology"
            },
            {
                "CourseID": 103,
                "CourseCode": "EE301",
                "CourseName": "Circuit Analysis",
                "CreditHours": 4,
                "Department": "Electrical Engineering"
            },
            {
                "CourseID": 104,
                "CourseCode": "MATH202",
                "CourseName": "Linear Algebra",
                "CreditHours": 3,
                "Department": "Mathematics"
            },
            {
                "CourseID": 105,
                "CourseCode": "MECH310",
                "CourseName": "Thermodynamics",
                "CreditHours": 4,
                "Department": "Mechanical Engineering"
            },
            {
                "CourseID": 106,
                "CourseCode": "PSY101",
                "CourseName": "Introduction to Psychology",
                "CreditHours": 3,
                "Department": "Psychology"
            },
            {
                "CourseID": 107,
                "CourseCode": "BUS201",
                "CourseName": "Principles of Management",
                "CreditHours": 3,
                "Department": "Business"
            },
            {
                "CourseID": 108,
                "CourseCode": "CHEM205",
                "CourseName": "Organic Chemistry",
                "CreditHours": 4,
                "Department": "Chemistry"
            },
            {
                "CourseID": 109,
                "CourseCode": "CS205",
                "CourseName": "Data Structures and Algorithms",
                "CreditHours": 4,
                "Department": "Computer Science"
            },
            {
                "CourseID": 110,
                "CourseCode": "BIO301",
                "CourseName": "Genetics",
                "CreditHours": 4,
                "Department": "Biology"
            }
        ]

    @staticmethod
    def get_enrollments_data():
        return [
            {"EnrollmentID": 1, "StudentID": 1001, "CourseID": 101, "Semester": "Fall", "Year": 2020, "Grade": "A"},
            {"EnrollmentID": 2, "StudentID": 1001, "CourseID": 109, "Semester": "Spring", "Year": 2021, "Grade": "A-"},
            {"EnrollmentID": 3, "StudentID": 1002, "CourseID": 102, "Semester": "Spring", "Year": 2021, "Grade": "A-"},
            {"EnrollmentID": 4, "StudentID": 1002, "CourseID": 110, "Semester": "Fall", "Year": 2021, "Grade": "B+"},
            {"EnrollmentID": 5, "StudentID": 1003, "CourseID": 103, "Semester": "Fall", "Year": 2020, "Grade": "B+"},
            {"EnrollmentID": 6, "StudentID": 1003, "CourseID": 101, "Semester": "Spring", "Year": 2021, "Grade": "C+"},
            {"EnrollmentID": 7, "StudentID": 1004, "CourseID": 104, "Semester": "Fall", "Year": 2021, "Grade": "A"},
            {"EnrollmentID": 8, "StudentID": 1004, "CourseID": 109, "Semester": "Spring", "Year": 2022, "Grade": "A"},
            {"EnrollmentID": 9, "StudentID": 1005, "CourseID": 105, "Semester": "Spring", "Year": 2020, "Grade": "B"},
            {"EnrollmentID": 10, "StudentID": 1005, "CourseID": 104, "Semester": "Fall", "Year": 2021, "Grade": "B+"},
            {"EnrollmentID": 11, "StudentID": 1006, "CourseID": 106, "Semester": "Fall", "Year": 2020, "Grade": "A-"},
            {"EnrollmentID": 12, "StudentID": 1006, "CourseID": 107, "Semester": "Spring", "Year": 2021, "Grade": "B+"},
            {"EnrollmentID": 13, "StudentID": 1007, "CourseID": 107, "Semester": "Fall", "Year": 2020, "Grade": "A"},
            {"EnrollmentID": 14, "StudentID": 1007, "CourseID": 101, "Semester": "Spring", "Year": 2021, "Grade": "B"},
            {"EnrollmentID": 15, "StudentID": 1008, "CourseID": 108, "Semester": "Fall", "Year": 2021, "Grade": "A"},
            {"EnrollmentID": 16, "StudentID": 1008, "CourseID": 102, "Semester": "Spring", "Year": 2022, "Grade": "A-"}
        ]

    @staticmethod
    def get_hackathon_participations():
        return [
            {
                "ActivityID": "HACK001",
                "StudentID": 1001,
                "HackathonName": "Annual University Hackathon",
                "TeamName": "Code Wizards",
                "ProjectTitle": "AI Tutor System",
                "ProjectDescription": "An AI-powered learning assistant for students",
                "ParticipationDate": datetime(2020, 10, 15),
                "AwardsWon": ["Best Innovation Award"],
                "SkillsUsed": ["Python", "Machine Learning", "Natural Language Processing"]
            },
            {
                "ActivityID": "HACK002",
                "StudentID": 1002,
                "HackathonName": "BioTech Challenge",
                "TeamName": "Gene Hackers",
                "ProjectTitle": "DNA Sequence Analyzer",
                "ProjectDescription": "Tool for analyzing DNA sequences for mutations",
                "ParticipationDate": datetime(2021, 3, 22),
                "AwardsWon": ["Best Scientific Application"],
                "SkillsUsed": ["Python", "Bioinformatics", "Data Analysis"]
            },
            {
                "ActivityID": "HACK003",
                "StudentID": 1004,
                "HackathonName": "Math Modeling Hackathon",
                "TeamName": "MathMagicians",
                "ProjectTitle": "Traffic Flow Optimization",
                "ProjectDescription": "Modeling urban traffic to minimize congestion",
                "ParticipationDate": datetime(2022, 1, 20),
                "AwardsWon": [],
                "SkillsUsed": ["Mathematical Modeling", "Simulation", "Python"]
            },
            {
                "ActivityID": "HACK004",
                "StudentID": 1006,
                "HackathonName": "PsychTech Hackathon",
                "TeamName": "MindMeld",
                "ProjectTitle": "Mental Health Chatbot",
                "ProjectDescription": "AI chatbot for preliminary mental health screening",
                "ParticipationDate": datetime(2021, 11, 12),
                "AwardsWon": ["Best Social Impact"],
                "SkillsUsed": ["Psychology", "NLP", "UX Design"]
            },
            {
                "ActivityID": "HACK005",
                "StudentID": 1008,
                "HackathonName": "ChemHack",
                "TeamName": "LabRats",
                "ProjectTitle": "Chemical Reaction Simulator",
                "ProjectDescription": "Interactive simulation of organic chemistry reactions",
                "ParticipationDate": datetime(2022, 2, 18),
                "AwardsWon": ["Best Educational Tool"],
                "SkillsUsed": ["Chemistry", "JavaScript", "Visualization"]
            }
        ]

    @staticmethod
    def get_sports_participations():
        return [
            {
                "ActivityID": "SPORT001",
                "StudentID": 1003,
                "SportName": "Basketball",
                "TeamName": "University Eagles",
                "Position": "Point Guard",
                "Season": "Winter 2020-2021",
                "MatchesPlayed": 15,
                "Achievements": ["Conference Champions", "MVP Award"]
            },
            {
                "ActivityID": "SPORT002",
                "StudentID": 1005,
                "SportName": "Soccer",
                "TeamName": "Mechanical Mavericks",
                "Position": "Midfielder",
                "Season": "Spring 2021",
                "MatchesPlayed": 10,
                "Achievements": ["Top Scorer"]
            },
            {
                "ActivityID": "SPORT003",
                "StudentID": 1007,
                "SportName": "Tennis",
                "TeamName": "University Racquet Club",
                "Position": "Singles Player",
                "Season": "Fall 2020",
                "MatchesPlayed": 8,
                "Achievements": ["Regional Finalist"]
            },
            {
                "ActivityID": "SPORT004",
                "StudentID": 1002,
                "SportName": "Swimming",
                "TeamName": "Aqua Eagles",
                "Position": "Freestyle",
                "Season": "Winter 2021-2022",
                "MatchesPlayed": 6,
                "Achievements": ["3 Gold Medals"]
            }
        ]

    @staticmethod
    def get_student_clubs():
        return [
            {
                "MembershipID": "CLUB001",
                "StudentID": 1001,
                "ClubName": "Computer Science Society",
                "Role": "President",
                "JoinDate": datetime(2018, 9, 10),
                "Active": True,
                "MeetingsAttended": 24
            },
            {
                "MembershipID": "CLUB002",
                "StudentID": 1002,
                "ClubName": "Biology Students Association",
                "Role": "Treasurer",
                "JoinDate": datetime(2019, 1, 15),
                "Active": True,
                "MeetingsAttended": 18
            },
            {
                "MembershipID": "CLUB003",
                "StudentID": 1004,
                "ClubName": "Math Club",
                "Role": "Member",
                "JoinDate": datetime(2020, 9, 5),
                "Active": True,
                "MeetingsAttended": 12
            },
            {
                "MembershipID": "CLUB004",
                "StudentID": 1005,
                "ClubName": "Robotics Club",
                "Role": "Vice President",
                "JoinDate": datetime(2019, 2, 20),
                "Active": True,
                "MeetingsAttended": 20
            },
            {
                "MembershipID": "CLUB005",
                "StudentID": 1006,
                "ClubName": "Psychology Society",
                "Role": "Event Coordinator",
                "JoinDate": datetime(2019, 9, 15),
                "Active": True,
                "MeetingsAttended": 16
            },
            {
                "MembershipID": "CLUB006",
                "StudentID": 1007,
                "ClubName": "Entrepreneurship Club",
                "Role": "President",
                "JoinDate": datetime(2017, 1, 20),
                "Active": True,
                "MeetingsAttended": 30
            },
            {
                "MembershipID": "CLUB007",
                "StudentID": 1008,
                "ClubName": "Chemistry Club",
                "Role": "Lab Assistant",
                "JoinDate": datetime(2020, 2, 10),
                "Active": True,
                "MeetingsAttended": 14
            }
        ]

    @staticmethod
    def get_metagraph_data():
        return {
            "nodes": [
                {"id": "n1", "type": "table", "label": "Students", "datasource": "UniversityDB"},
                {"id": "n2", "type": "table", "label": "Courses", "datasource": "UniversityDB"},
                {"id": "n3", "type": "table", "label": "Enrollments", "datasource": "UniversityDB"},
                {"id": "n4", "type": "collection", "label": "HackathonParticipations", "datasource": "UniversityActivities"},
                {"id": "n5", "type": "collection", "label": "SportsParticipations", "datasource": "UniversityActivities"},
                {"id": "n6", "type": "collection", "label": "StudentClubs", "datasource": "UniversityActivities"}
            ],
            "edges": [
                {"source": "n3", "target": "n1", "relation": "references"},
                {"source": "n3", "target": "n2", "relation": "references"},
                {"source": "n4", "target": "n1", "relation": "references"},
                {"source": "n5", "target": "n1", "relation": "references"},
                {"source": "n6", "target": "n1", "relation": "references"}
            ]
        }