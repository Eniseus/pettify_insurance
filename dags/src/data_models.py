from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional, Union

@dataclass
class Customer:
    customer_id: str
    first_name: str
    last_name: str
    email: str
    phone: str
    address: str
    city: str
    birthdate: date
    registration_date: date
    customer_status: str

@dataclass
class Pet:
    pet_id: str
    customer_id: str
    pet_name: str
    pet_type: str
    breed: str
    date_of_birth: date
    gender: str
    weight: float
    vaccination: bool
    spayed_neutered: bool

@dataclass
class Policy:
    policy_id: str
    pet_id: str
    customer_id: str
    policy_type: str
    start_date: date
    end_date: date
    monthly_premium: float
    coverage_limit: Union[int, str]
    policy_status: str

@dataclass
class Claim:
    claim_id: str
    policy_id: str
    pet_id: str
    customer_id: str
    claim_date: date
    claim_type: str
    claim_amount: float
    claim_status: str
    submitted_date: date
    processed_date: Optional[date]

TABLE_SCHEMAS = {
    'customers': """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id VARCHAR(20) PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            email VARCHAR(200) UNIQUE NOT NULL,
            phone VARCHAR(20),
            address TEXT,
            city VARCHAR(100),
            birthdate DATE,
            registration_date DATE,
            customer_status VARCHAR(20)
        );
    """,
    
    'pets': """
        CREATE TABLE IF NOT EXISTS pets (
            pet_id VARCHAR(20) PRIMARY KEY,
            customer_id VARCHAR(20) REFERENCES customers(customer_id),
            pet_name VARCHAR(50) NOT NULL,
            pet_type VARCHAR(50),
            breed VARCHAR(100),
            date_of_birth DATE,
            gender CHAR(1) CHECK (gender IN ('M', 'F')),
            weight DECIMAL(5,2),
            vaccination BOOLEAN,
            spayed_neutered BOOLEAN
        );
    """,
    
    'policies': """
        CREATE TABLE IF NOT EXISTS policies (
            policy_id VARCHAR(20) PRIMARY KEY,
            pet_id VARCHAR(20) REFERENCES pets(pet_id),
            customer_id VARCHAR(20) REFERENCES customers(customer_id),
            policy_type VARCHAR(20),
            start_date DATE,
            end_date DATE,
            monthly_premium DECIMAL(8,2),
            coverage_limit VARCHAR(20),
            policy_status VARCHAR(20)
        );
    """,
    
    'claims': """
        CREATE TABLE IF NOT EXISTS claims (
            claim_id VARCHAR(20) PRIMARY KEY,
            policy_id VARCHAR(20) REFERENCES policies(policy_id),
            pet_id VARCHAR(20) REFERENCES pets(pet_id),
            customer_id VARCHAR(20) REFERENCES customers(customer_id),
            claim_date DATE,
            claim_type VARCHAR(50),
            claim_amount DECIMAL(10,2),
            claim_status VARCHAR(20),
            submitted_date DATE,
            processed_date DATE
        );
    """
}
