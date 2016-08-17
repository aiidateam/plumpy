

import plum.process_database
import plum.simple_database

# By default there is a plum uses the simple database with no retention of
# process outputs as this could baloon out of control.  Users are free
# to set the database as they like based on their needs.
plum.process_database.set_db(
    plum.simple_database.SimpleDatabase(retain_outputs=False))