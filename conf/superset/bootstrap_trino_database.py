import os
import time

from superset.app import create_app

catalog=os.getenv("ICEBERG_NAMESPACE")
TENANT_CATALOGS = [catalog]


def create_tenant_database(app, catalog_name: str) -> None:
    """Create a Superset database for a tenant catalog."""
    from superset.extensions import db
    from superset.models.core import Database

    database_name = catalog_name.upper()
    sqlalchemy_uri = f"trino://admin:admin@trino:8443/{catalog_name}?verify=false"

    database = (
        db.session.query(Database)
        .filter(Database.database_name == database_name)
        .one_or_none()
    )

    if database is None:
        database = Database(database_name=database_name)

    database.set_sqlalchemy_uri(sqlalchemy_uri)
    database.expose_in_sqllab = True

    db.session.add(database)
    db.session.commit()
    print(f"✓ Superset database '{database_name}' configured (catalog: {catalog_name})")


def main() -> None:
    app = create_app()
    with app.app_context():
        from superset.extensions import db

        print("Registering tenant catalogs in Superset...")

        # Register each tenant catalog as a separate database
        for catalog in TENANT_CATALOGS:
            try:
                create_tenant_database(app, catalog)
            except Exception as e:
                print(f"! Warning: Could not register catalog '{catalog}': {e}")

        # Also register the default iceberg catalog for backward compatibility
        try:
            from superset.models.core import Database

            default_name = "DataWarehouse"
            default_uri = "trino://admin:admin@trino:8443/iceberg?verify=false"

            database = (
                db.session.query(Database)
                .filter(Database.database_name == default_name)
                .one_or_none()
            )

            if database is None:
                database = Database(database_name=default_name)

            database.set_sqlalchemy_uri(default_uri)
            database.expose_in_sqllab = True

            db.session.add(database)
            db.session.commit()
            print(f"✓ Superset database '{default_name}' configured (catalog: iceberg)")
        except Exception as e:
            print(f"! Warning: Could not register default iceberg catalog: {e}")

        print("\nAll tenant catalogs registered in Superset.")


if __name__ == "__main__":
    main()
