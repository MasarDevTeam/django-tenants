from django.core.exceptions import ValidationError
from django.db import connection, transaction
from django.db.utils import ProgrammingError

from django_tenants.utils import schema_exists
from .clone_script import CLONE_SCHEMA_FUNCTION

class CloneSchema:

    def _create_clone_schema_function(self):
        """
        Creates a postgres function `clone_schema` that copies a schema and its
        contents. Will replace any existing `clone_schema` functions owned by the
        `postgres` superuser.
        """
        cursor = connection.cursor()
        cursor.execute(CLONE_SCHEMA_FUNCTION)
        cursor.close()

    def clone_schema(self, base_schema_name, new_schema_name, set_connection=True):
        """
        Creates a new schema `new_schema_name` as a clone of an existing schema
        `old_schema_name`.
        """
        if set_connection:
            connection.set_schema_to_public()
        cursor = connection.cursor()

        # check if the clone_schema function already exists in the db
        try:
            cursor.execute("SELECT 'clone_schema'::regproc")
            self._create_clone_schema_function()
        except ProgrammingError:
            self._create_clone_schema_function()
            transaction.commit()

        if schema_exists(new_schema_name):
            raise ValidationError("New schema name already exists")

        sql = "SELECT clone_schema(%(base_schema)s, %(new_schema)s,'DATA','NOACL','VERBOSE')"
        cursor.execute(
            sql,
            {'base_schema': base_schema_name, 'new_schema': new_schema_name}
        )
        cursor.close()
