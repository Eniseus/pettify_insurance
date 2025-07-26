import os, random, psycopg2
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from .data_models import TABLE_SCHEMAS

fake = Faker(locale='de_DE')

class PettifyDataGenerator:
    
    def __init__(self, num_customers: int = 1000):
        self.num_customers = num_customers
        self.pet_breeds = {
            'Dog': ['Labrador', 'Golden Retriever', 'German Shepherd', 'French Bulldog', 'Standard Poodle', 
                   'Beagle', 'Rottweiler', 'Yorkshire Terrier', 'Newfoundland', 'Mixed/unknown breed'],
            'Cat': ['Persian', 'Norwegian Forest', 'Maine Coon', 'British Shorthair', 'Ragdoll',
                   'Bengal', 'Exotic Shorthair', 'Sphynx', 'Turkish Angora', 'Mixed/unknown breed'],
        }

        self.policy_types = ['Basic', 'Premium']
        self.claim_types = ['Illness', 'Injury', 'Routine Care', 'Surgery']
        
    def generate_customers(self) -> pd.DataFrame:
        customers = []
        
        for i in range(self.num_customers):
            customer = {
                'customer_id': f'cust_{i+1:06d}',
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'email': fake.email(),
                'phone': fake.phone_number(),
                'address': fake.street_address(),
                'city': fake.city(),
                'birthdate': fake.date_of_birth(minimum_age=18, maximum_age=100),
                'registration_date': fake.date_between(start_date='-2y', end_date='today'),
                'customer_status': random.choice(['Active', 'Inactive', 'Suspended']),
            }
            customers.append(customer)
            
        return pd.DataFrame(customers)
    
    def generate_pets(self, customers_df: pd.DataFrame) -> pd.DataFrame:
        pets = []
        pet_id_counter = 1
        
        for _, customer in customers_df.iterrows():
            num_pets = random.choices([1, 2, 3], weights=[0.6, 0.3, 0.1])[0]
            
            for _ in range(num_pets):
                pet_type = random.choice(list(self.pet_breeds.keys()))
                breed = random.choice(self.pet_breeds[pet_type])
                
                pet = {
                    'pet_id': f'pet_{pet_id_counter:06d}',
                    'customer_id': customer['customer_id'],
                    'pet_name': fake.first_name(),
                    'pet_type': pet_type,
                    'breed': breed,
                    'date_of_birth': fake.date_between(start_date='-15y', end_date='-2w'), # pets between 2 weeks and 15 years old
                    'gender': random.choice(['M', 'F']),
                    'weight': round(random.uniform(1, 70), 2),  
                    'vaccination': random.choice([True, False]),
                    'spayed_neutered': random.choice([True, False]),
                }
                pets.append(pet)
                pet_id_counter += 1
                
        return pd.DataFrame(pets)
    
    def generate_policies(self, pets_df: pd.DataFrame) -> pd.DataFrame:
        policies = []
        
        for _, pet in pets_df.iterrows():
            if random.random() > 0.2:
                policy_type = random.choice(self.policy_types)
                
                base_premium = {
                    'Basic': 25, 'Premium': 75
                }[policy_type]
                
                age_factor = 1.0
                if pet['pet_type'] in ['Dog', 'Cat']:
                    pet_age = (datetime.now().date() - pet['date_of_birth']).days / 365
                    if pet_age > 7:
                        age_factor = 1.5
                    elif pet_age > 3:
                        age_factor = 1.2
                
                monthly_premium = round(base_premium * age_factor * random.uniform(0.8, 1.2), 2)
                
                start_date = fake.date_between(start_date='-2y', end_date='today')
                end_date = start_date + timedelta(days=365)

                policy_status = 'Inactive' if datetime.now().date() > end_date else 'Active'
                
                policy = {
                    'policy_id': f'pol_{fake.uuid4()[:8]}',
                    'pet_id': pet['pet_id'],
                    'customer_id': pet['customer_id'],
                    'policy_type': policy_type,
                    'start_date': start_date,
                    'end_date': end_date,
                    'monthly_premium': monthly_premium,
                    'coverage_limit': random.choice([5000, 10000, 15000, 25000, 'Unlimited']),
                    'reimbursement_rate': random.choice([0.7, 0.8, 0.9]),
                    'policy_status': policy_status,
                }
                policies.append(policy)
                
        return pd.DataFrame(policies)
    
    def generate_claims(self, policies_df: pd.DataFrame) -> pd.DataFrame:
        claims = []
        claim_id_counter = 1
        
        for _, policy in policies_df.iterrows():
            
            num_claims = random.choices([0, 1, 2, 3, 4, 5], weights=[0.4, 0.3, 0.15, 0.1, 0.04, 0.01])[0]
            
            for _ in range(num_claims):
                claim_date = fake.date_between(start_date=policy['start_date'], end_date='today')
                claim_type = random.choice(self.claim_types)
                
                amount_ranges = {
                    'Routine Care': (50, 300),
                    'Illness': (100, 2000),
                    'Injury': (200, 5000),
                    'Surgery': (1000, 15000)
                }
                
                claim_amount = round(random.uniform(*amount_ranges[claim_type]), 2)
                
                claim = {
                    'claim_id': f'cla_{claim_id_counter:06d}',
                    'policy_id': policy['policy_id'],
                    'pet_id': policy['pet_id'],
                    'customer_id': policy['customer_id'],
                    'claim_date': claim_date,
                    'claim_type': claim_type,
                    'claim_amount': claim_amount,
                    'claim_status': random.choice(['Pending', 'Approved', 'Denied', 'Paid']),
                    'submitted_date': claim_date + timedelta(days=random.randint(0, 7)),
                    'processed_date': claim_date + timedelta(days=random.randint(1, 30)) if random.random() > 0.2 else None,
                }
                claims.append(claim)
                claim_id_counter += 1
                
        return pd.DataFrame(claims)
    
    def connect_db(self):
        db_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'pettify'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', 'password')
        }
        
        connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        return create_engine(connection_string)
    
    def create_tables(self):
        engine = self.connect_db()
        
        with engine.connect() as connection:
            for table_name, schema_sql in TABLE_SCHEMAS.items():
                connection.execute(text(schema_sql))
                connection.commit()
        
    def save_to_db(self, dataframe: pd.DataFrame, table_name: str):
        engine = self.connect_db()
        dataframe.to_sql(table_name, engine, if_exists='append', index=False)
    
    def save_to_csv(self, dataframe: pd.DataFrame, filename: str):
        filepath = f"/tmp/{filename}"
        dataframe.to_csv(filepath, index=False)
        return filepath

def generate_data():

    generator = PettifyDataGenerator(num_customers=1000)
    
    try:
        generator.create_tables()
        customers_df = generator.generate_customers()
        pets_df = generator.generate_pets(customers_df)
        policies_df = generator.generate_policies(pets_df)
        claims_df = generator.generate_claims(policies_df)
        generator.save_to_db(customers_df, 'customers')
        generator.save_to_db(pets_df, 'pets')
        generator.save_to_db(policies_df, 'policies')
        generator.save_to_db(claims_df, 'claims')
        generator.save_to_csv(customers_df, 'customers.csv')
        generator.save_to_csv(pets_df, 'pets.csv')
        generator.save_to_csv(policies_df, 'policies.csv')
        generator.save_to_csv(claims_df, 'claims.csv')
        
    except Exception as e:
        print(f"Data generation error: {str(e)}")
        raise

if __name__ == "__main__":
    generate_data()