"""
Unit tests for the database helper Sqlite3 class
"""

import os
from unittest import main, TestCase

from database_helper import Sqlite3

DATABASE = 'example.db'
TABLE = 'epl_03_04'


class Sqlite3Tests(TestCase):
    """Tests for the Sqlite3 class"""
    @classmethod
    def setUpClass(cls):
        """Creates the database"""
        cls.database = Sqlite3(DATABASE)
        populate_database(cls.database)

    @classmethod
    def tearDownClass(cls):
        """Deletes the database"""
        os.remove(DATABASE)

    def test_01_select(self):
        """Tests that the select method works and defaults to selecting all
        the columns"""
        all_teams = self.database.select(table=TABLE)
        self.assertEqual(len(all_teams), 20)
        for team_details in all_teams:
            self.assertEqual(len(team_details), 10)

    def test_02_select_column(self):
        """Tests that the select method works when selecting a single column"""
        positions = self.database.select(table=TABLE, columns=['Pos'])

        for index, position in enumerate(positions):
            self.assertEqual(position[0], index+1)

    def test_03_select_specific_row(self):
        """Tests that the select method works when selecting a specific row"""
        team = self.database.select(
            table=TABLE,
            columns=['team'],
            key_name='Pos',
            key_value=1
        )
        self.assertEqual(team[0][0], 'Arsenal')  # Come on you Gunners!

    def test_04_select_ordered(self):
        """Tests that the select method works when ordering the data"""
        teams = self.database.select(
            table=TABLE,
            columns=['Team'],
            order_column='GA',
            direction='ASC'
        )

        fewest_goals_against = 'Arsenal'
        most_goals_against = 'Leeds United'

        self.assertEqual(teams[0][0], fewest_goals_against)
        self.assertEqual(teams[19][0], most_goals_against)

    def test_05_select_limited(self):
        """Tests that the select method works when limiting the data"""
        data = self.database.select(
            table=TABLE,
            columns=['Pos', 'Team', 'Pts'],
            limit=10
        )
        self.assertEqual(len(data), 10)

    def test_06_update(self):
        """Tests that the update method works"""
        # Let's pretend that Tottenham got relegated
        self.database.update(
            table=TABLE,
            details={'Pos': 20},
            key_name='Team',
            key_value='Tottenham'
        )
        self.database.update(
            table=TABLE,
            details={'Pos': 14},
            key_name='Team',
            key_value='Wolves'
        )

        relegated_teams = self.database.select(
            table=TABLE,
            columns=['team'],
            order_column='Pos',
            direction='DESC',
            limit=3
        )
        self.assertEqual(len(relegated_teams), 3)
        self.assertEqual(relegated_teams[0][0], 'Tottenham')
        self.assertEqual(relegated_teams[1][0], 'Leeds United')
        self.assertEqual(relegated_teams[2][0], 'Leicester City')

    def test_07_execute_sql(self):
        """Tests that the execute sql method works by dropping the table"""
        sql_statement = f'DROP TABLE {TABLE}'
        self.database.execute_sql(sql_statement)

        # Try to select and ensure that an error is raised
        with self.assertRaises(Exception) as context:
            self.database.select(
                table=TABLE,
                columns=['Team']
            )

            self.assertEqual(f'no such table: {TABLE}', str(context))


def populate_database(database):  # pylint: disable=too-many-locals
    """Populates the passed database

    Args:
        database (Sqlite3): Sqlite3 database helper object
    """
    sql_statement = (
        """CREATE TABLE IF NOT EXISTS epl_03_04 (
           Pos INTEGER NOT NULL,
           Team TEXT NOT NULL,
           Pld INTEGER NOT NULL,
           W INTEGER NOT NULL,
           D INTEGER NOT NULL,
           L INTEGER NOT NULL,
           GF INTEGER NOT NULL,
           GA INTEGER NOT NULL,
           GD INTEGER NOT NULL,
           Pts INTEGER NOT NULL);"""
    )
    database.execute_sql(sql_statement)

    arsenal = {
        'Pos': 1, 'Team': 'Arsenal', 'Pld': 38, 'W': 26,
        'D': 12, 'L': 0, 'GF': 73, 'GA': 26, 'GD': 47, 'Pts': 90
    }
    chelsea = {
        'Pos': 2, 'Team': 'Chelsea', 'Pld': 38, 'W': 24,
        'D': 7, 'L': 7, 'GF': 67, 'GA': 30, 'GD': 37, 'Pts': 79
    }
    man_united = {
        'Pos': 3, 'Team': 'Manchester United', 'Pld': 38, 'W': 23,
        'D': 6, 'L': 9, 'GF': 64, 'GA': 35, 'GD': 29, 'Pts': 75
    }
    liverpool = {
        'Pos': 4, 'Team': 'Liverpool', 'Pld': 38, 'W': 16,
        'D': 12, 'L': 10, 'GF': 55, 'GA': 37, 'GD': 18, 'Pts': 60
    }
    newcastle = {
        'Pos': 5, 'Team': 'Newcastle', 'Pld': 38, 'W': 13,
        'D': 17, 'L': 8, 'GF': 52, 'GA': 40, 'GD': 12, 'Pts': 56
    }
    aston_villa = {
        'Pos': 6, 'Team': 'Aston Villa', 'Pld': 38, 'W': 15,
        'D': 11, 'L': 12, 'GF': 48, 'GA': 44, 'GD': 4, 'Pts': 56
    }
    charlton = {
        'Pos': 7, 'Team': 'Charlton', 'Pld': 38, 'W': 14,
        'D': 11, 'L': 13, 'GF': 51, 'GA': 51, 'GD': 0, 'Pts': 53
    }
    bolton = {
        'Pos': 8, 'Team': 'Bolton', 'Pld': 38, 'W': 14,
        'D': 11, 'L': 13, 'GF': 48, 'GA': 56, 'GD': -8, 'Pts': 53
    }
    fulham = {
        'Pos': 9, 'Team': 'Fulham', 'Pld': 38, 'W': 14,
        'D': 10, 'L': 14, 'GF': 52, 'GA': 46, 'GD': 6, 'Pts': 52
    }
    birmingham = {
        'Pos': 10, 'Team': 'Birmingham', 'Pld': 38, 'W': 12,
        'D': 14, 'L': 12, 'GF': 43, 'GA': 48, 'GD': -5, 'Pts': 50
    }
    middlesbrough = {
        'Pos': 11, 'Team': 'Middlesbrough', 'Pld': 38, 'W': 13,
        'D': 9, 'L': 16, 'GF': 44, 'GA': 52, 'GD': -8, 'Pts': 48
    }
    southampton = {
        'Pos': 12, 'Team': 'Southampton', 'Pld': 38, 'W': 12,
        'D': 11, 'L': 15, 'GF': 44, 'GA': 45, 'GD': -1, 'Pts': 47
    }
    portsmouth = {
        'Pos': 13, 'Team': 'Portsmouth', 'Pld': 38, 'W': 12,
        'D': 9, 'L': 17, 'GF': 47, 'GA': 54, 'GD': -7, 'Pts': 45
    }
    tottenham = {
        'Pos': 14, 'Team': 'Tottenham', 'Pld': 38, 'W': 13,
        'D': 6, 'L': 19, 'GF': 47, 'GA': 57, 'GD': -10, 'Pts': 45
    }
    blackburn = {
        'Pos': 15, 'Team': 'Blackburn Rovers', 'Pld': 38, 'W': 12,
        'D': 8, 'L': 18, 'GF': 51, 'GA': 59, 'GD': -8, 'Pts': 44
    }
    man_city = {
        'Pos': 16, 'Team': 'Manchester City', 'Pld': 38, 'W': 9,
        'D': 14, 'L': 15, 'GF': 55, 'GA': 54, 'GD': 1, 'Pts': 41
    }
    everton = {
        'Pos': 17, 'Team': 'Everton', 'Pld': 38, 'W': 9,
        'D': 12, 'L': 17, 'GF': 45, 'GA': 57, 'GD': -12, 'Pts': 39
    }
    leicester = {
        'Pos': 18, 'Team': 'Leicester City', 'Pld': 38, 'W': 6,
        'D': 15, 'L': 17, 'GF': 48, 'GA': 65, 'GD': -17, 'Pts': 33
    }
    leeds = {
        'Pos': 19, 'Team': 'Leeds United', 'Pld': 38, 'W': 8,
        'D': 9, 'L': 21, 'GF': 40, 'GA': 79, 'GD': -39, 'Pts': 33
    }
    wolves = {
        'Pos': 20, 'Team': 'Wolves', 'Pld': 38, 'W': 7,
        'D': 12, 'L': 19, 'GF': 38, 'GA': 77, 'GD': -39, 'Pts': 33
    }

    all_teams = [
        arsenal, chelsea, man_united, liverpool, newcastle, aston_villa,
        charlton, bolton, fulham, birmingham, middlesbrough, southampton,
        portsmouth, tottenham, blackburn, man_city, everton, leicester,
        leeds, wolves
    ]

    for team in all_teams:
        database.insert(table=TABLE, details=team)


if __name__ == '__main__':
    main()
