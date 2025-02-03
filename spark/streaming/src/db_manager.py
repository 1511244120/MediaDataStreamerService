from typing import List, Dict, Type
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from models.base import Base  # Assume this is where the Base ORM is defined

class PostgresDataManager:
    """
    A class for handling database operations in PostgreSQL using SQLAlchemy ORM.
    Supports inserting single/multiple records and updating records.
    """
    
    def __init__(self, db_config: Dict[str, str]):
        """
        Initialize the database connection and session.
        :param db_config: A dictionary containing database connection parameters.
        """
        self.engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
    
    def close(self) -> None:
        """
        Close the database session and dispose of the engine.
        """
        self.session.close()
        self.engine.dispose()
    
    def insert_record(self, model: Type[Base], record: Dict[str, any]) -> None:
        """
        Insert a single record into the specified table.
        :param model: ORM model class corresponding to the table.
        :param record: A dictionary containing data for the record to insert.
        """
        try:
            new_record = model(**record)
            self.session.add(new_record)
            self.session.commit()
            print(f"Successfully inserted a record into {model.__tablename__}.")
        except SQLAlchemyError as e:
            self.session.rollback()
            print(f"Error inserting record into {model.__tablename__}: {e}")
            raise
    
    def insert(self, model: Type[Base], data: List[Dict[str, any]]) -> None:
        """
        Insert multiple records into the specified table.
        :param model: ORM model class corresponding to the table.
        :param data: List of dictionaries containing data for multiple records.
        """
        try:
            self.session.bulk_insert_mappings(model, data)
            self.session.commit()
            print(f"Successfully inserted {len(data)} records into {model.__tablename__}.")
        except SQLAlchemyError as e:
            self.session.rollback()
            print(f"Error inserting records into {model.__tablename__}: {e}")
            raise
    
    def update(self, model: Type[Base], conflict_column: str, update_data: Dict[str, any]) -> None:
        """
        Update a record in the specified table.
        :param model: ORM model class corresponding to the table.
        :param conflict_column: Column to check for conflicts (usually the primary key).
        :param update_data: Dictionary containing the updated data.
        """
        try:
            record = self.session.query(model).filter(
                getattr(model, conflict_column) == update_data[conflict_column]
            ).one_or_none()
            
            if record:
                for key, value in update_data.items():
                    setattr(record, key, value)
                print(f"Updated record where {conflict_column} = {update_data[conflict_column]}")
            else:
                print(f"No record found where {conflict_column} = {update_data[conflict_column]}")
            
            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            print(f"Error updating {model.__tablename__}: {e}")
            raise