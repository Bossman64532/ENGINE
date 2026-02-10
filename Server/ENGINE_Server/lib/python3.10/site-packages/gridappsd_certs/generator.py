"""
Device Certificate Generator

A Python library for generating X.509 certificates for IoT and smart grid devices.
Supports TLS 1.2 requirements with AES-CCM encryption capabilities, suitable for
IEEE 2030.5, general IoT devices, and other secure communication protocols.
"""

import datetime
from ipaddress import ip_address
from enum import Enum
import uuid
from cryptography import x509
from cryptography.x509.oid import NameOID, ExtensionOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, ec
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.x509 import ExtendedKeyUsageOID


class ContentType(Enum):
    """Supported content types for IEEE 2030.5 communications."""
    XML = "application/xml"
    JSON = "application/json"
    EXI = "application/exi"  # Efficient XML Interchange
class DeviceCertificateGenerator:
    """Generator for X.509 certificates for secure device communications."""
    
    def __init__(self, key_type='rsa', key_size=2048, ec_curve=None):
        """
        Initialize the certificate generator.
        
        Args:
            key_type (str): Type of key to generate ('rsa' or 'ec')
            key_size (int): Size of RSA key in bits (default: 2048)
            ec_curve: EC curve to use if key_type is 'ec' (default: SECP256R1)
        """
        valid_key_types = ['rsa', 'ec']
        if key_type.lower() not in valid_key_types:
            raise ValueError(f"Invalid key type: {key_type}. Must be one of: {', '.join(valid_key_types)}")
        
        self.key_type = key_type.lower()
        self.key_size = key_size
        self.ec_curve = ec_curve or ec.SECP256R1()
        
    def generate_private_key(self):
        """Generate a new private key based on configuration."""
        if self.key_type == 'rsa':
            return rsa.generate_private_key(
                public_exponent=65537,
                key_size=self.key_size
            )
        elif self.key_type == 'ec':
            return ec.generate_private_key(self.ec_curve)
        else:
            raise ValueError(f"Unsupported key type: {self.key_type}")
    
    def create_ca_certificate(self, subject_attributes, issuer_attributes=None, 
                              valid_days=3652, private_key=None):
        """
        Create a CA certificate (self-signed if issuer_attributes is None).
        
        Args:
            subject_attributes (dict): Dictionary with subject fields
            issuer_attributes (dict, optional): Dictionary with issuer fields for CA cert
            valid_days (int): Validity period in days (default: 10 years)
            private_key: Optional private key, otherwise a new one is generated
            
        Returns:
            tuple: (certificate, private_key)
        """
        private_key = private_key or self.generate_private_key()
        public_key = private_key.public_key()
        
        # Use provided issuer or make it self-signed
        if issuer_attributes is None:
            issuer_attributes = subject_attributes
            issuer_key = private_key
        else:
            # In this case, issuer_key would need to be provided separately
            raise ValueError("If issuer_attributes is provided, issuer_key must also be provided")
        
        subject = self._build_name(subject_attributes)
        issuer = self._build_name(issuer_attributes)
        
        # Certificate serial number
        serial = x509.random_serial_number()
        
        # Build the certificate
        cert_builder = x509.CertificateBuilder().subject_name(
            subject
        ).issuer_name(
            issuer
        ).public_key(
            public_key
        ).serial_number(
            serial
        ).not_valid_before(
            datetime.datetime.utcnow()
        ).not_valid_after(
            datetime.datetime.utcnow() + datetime.timedelta(days=valid_days)
        ).add_extension(
            x509.BasicConstraints(ca=True, path_length=None), critical=True
        ).add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=True,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True, 
                encipher_only=False,
                decipher_only=False
            ), critical=True
        ).add_extension(
            x509.SubjectKeyIdentifier.from_public_key(public_key), critical=False
        )
        
        # Add Authority Key Identifier if different from subject
        if issuer_attributes != subject_attributes:
            cert_builder = cert_builder.add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_public_key(issuer_key.public_key()),
                critical=False
            )
        
        # Sign the certificate with the issuer's private key
        certificate = cert_builder.sign(
            private_key=issuer_key, 
            algorithm=hashes.SHA256()
        )
        
        return certificate, private_key
    
    def create_device_certificate(self, subject_attributes, issuer_cert, issuer_key,
                                  valid_days=730, private_key=None, device_id=None, 
                                  san_type='uri', san_values=None):
        """
        Create a device certificate signed by a CA.
        
        Args:
            subject_attributes (dict): Dictionary with subject fields
            issuer_cert: Issuer CA certificate
            issuer_key: Issuer CA private key
            valid_days (int): Validity period in days (default: 2 years)
            private_key: Optional private key, otherwise a new one is generated
            device_id (str): Optional device ID, otherwise a UUID is generated
            san_type (str): Type of Subject Alternative Name ('uri', 'dns', 'ip', 'email')
            san_values (list): List of alternative names to include; if None and san_type is 'uri',
                              will generate a URI based on device_id
            
        Returns:
            tuple: (certificate, private_key)
        """
        if issuer_key is None:
            raise TypeError("Issuer private key cannot be None")
        
        private_key = private_key or self.generate_private_key()
        public_key = private_key.public_key()
        
        subject = self._build_name(subject_attributes)
        
        # Certificate serial number
        serial = x509.random_serial_number()
        
        # Device ID as a UUID (if not provided)
        if not device_id and san_type == 'uri' and not san_values:
            device_id = str(uuid.uuid4())
        
        # Build the certificate
        cert_builder = x509.CertificateBuilder().subject_name(
            subject
        ).issuer_name(
            issuer_cert.subject
        ).public_key(
            public_key
        ).serial_number(
            serial
        ).not_valid_before(
            datetime.datetime.utcnow()
        ).not_valid_after(
            datetime.datetime.utcnow() + datetime.timedelta(days=valid_days)
        ).add_extension(
            x509.BasicConstraints(ca=False, path_length=None), critical=True
        ).add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=True,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False, 
                encipher_only=False,
                decipher_only=False
            ), critical=True
        ).add_extension(
            x509.ExtendedKeyUsage([
                ExtendedKeyUsageOID.CLIENT_AUTH,
            ]), critical=True
        ).add_extension(
            x509.SubjectKeyIdentifier.from_public_key(public_key), critical=False
        ).add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(issuer_key.public_key()),
            critical=False
        )
        
        # Add subject alternative name based on type
        if san_values is None and san_type == 'uri' and device_id:
            san_values = [f"urn:uuid:{device_id}"]
        
        if san_values:
            san_objects = []
            if san_type == 'uri':
                san_objects = [x509.UniformResourceIdentifier(uri) for uri in san_values]
            elif san_type == 'dns':
                san_objects = [x509.DNSName(dns) for dns in san_values]
            elif san_type == 'ip':
                san_objects = [x509.IPAddress(ip_address(ip)) for ip in san_values]
            elif san_type == 'email':
                san_objects = [x509.RFC822Name(email) for email in san_values]
                
            cert_builder = cert_builder.add_extension(
                x509.SubjectAlternativeName(san_objects),
                critical=False
            )
        
        # Sign the certificate with the issuer's private key
        certificate = cert_builder.sign(
            private_key=issuer_key, 
            algorithm=hashes.SHA256()
        )
        
        return certificate, private_key
    
    def create_self_signed_device_cert(self, subject_attributes, valid_days=730, 
                                       private_key=None, device_id=None,
                                       san_type='uri', san_values=None):
        """
        Create a self-signed device certificate.
        
        Args:
            subject_attributes (dict): Dictionary with subject fields
            valid_days (int): Validity period in days (default: 2 years)
            private_key: Optional private key, otherwise a new one is generated
            device_id (str): Optional device ID, otherwise a UUID is generated
            san_type (str): Type of Subject Alternative Name ('uri', 'dns', 'ip', 'email')
            san_values (list): List of alternative names to include; if None and san_type is 'uri',
                              will generate a URI based on device_id
            
        Returns:
            tuple: (certificate, private_key)
        """
        private_key = private_key or self.generate_private_key()
        public_key = private_key.public_key()
        
        subject = self._build_name(subject_attributes)
        
        # Certificate serial number
        serial = x509.random_serial_number()
        
        # Device ID as a UUID (if not provided)
        if not device_id and san_type == 'uri' and not san_values:
            device_id = str(uuid.uuid4())
        
        # Build the certificate
        cert_builder = x509.CertificateBuilder().subject_name(
            subject
        ).issuer_name(
            subject  # Self-signed, so issuer = subject
        ).public_key(
            public_key
        ).serial_number(
            serial
        ).not_valid_before(
            datetime.datetime.utcnow()
        ).not_valid_after(
            datetime.datetime.utcnow() + datetime.timedelta(days=valid_days)
        ).add_extension(
            x509.BasicConstraints(ca=False, path_length=None), critical=True
        ).add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=True,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False, 
                encipher_only=False,
                decipher_only=False
            ), critical=True
        ).add_extension(
            x509.ExtendedKeyUsage([
                ExtendedKeyUsageOID.CLIENT_AUTH,
                ExtendedKeyUsageOID.SERVER_AUTH,
            ]), critical=True
        ).add_extension(
            x509.SubjectKeyIdentifier.from_public_key(public_key), critical=False
        )
        
        # Add Authority Key Identifier (same as SubjectKeyIdentifier for self-signed)
        cert_builder = cert_builder.add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(public_key),
            critical=False
        )
        
        # Add subject alternative name based on type
        if san_values is None and san_type == 'uri' and device_id:
            san_values = [f"urn:uuid:{device_id}"]
        
        if san_values:
            san_objects = []
            if san_type == 'uri':
                san_objects = [x509.UniformResourceIdentifier(uri) for uri in san_values]
            elif san_type == 'dns':
                san_objects = [x509.DNSName(dns) for dns in san_values]
            elif san_type == 'ip':
                san_objects = [x509.IPAddress(ip_address(ip)) for ip in san_values]
            elif san_type == 'email':
                san_objects = [x509.RFC822Name(email) for email in san_values]
                
            cert_builder = cert_builder.add_extension(
                x509.SubjectAlternativeName(san_objects),
                critical=False
            )
        
        # Sign the certificate with its own private key (self-signed)
        certificate = cert_builder.sign(
            private_key=private_key, 
            algorithm=hashes.SHA256()
        )
        
        return certificate, private_key
    
    def create_web_certificate(self, subject_attributes, issuer_cert, issuer_key,
                          valid_days=365, private_key=None, domains=None):
        """
        Create a certificate for web server use.
        
        Args:
            subject_attributes (dict): Dictionary with subject fields
            issuer_cert: Issuer CA certificate
            issuer_key: Issuer CA private key
            valid_days (int): Validity period in days (default: 1 year)
            private_key: Optional private key, otherwise a new one is generated
            domains (list): List of domain names to include in the certificate
            
        Returns:
            tuple: (certificate, private_key)
        """
        if issuer_key is None:
            raise TypeError("Issuer private key cannot be None")
        
        private_key = private_key or self.generate_private_key()
        public_key = private_key.public_key()
        
        subject = self._build_name(subject_attributes)
        
        # Certificate serial number
        serial = x509.random_serial_number()
        
        # Build the certificate
        cert_builder = x509.CertificateBuilder().subject_name(
            subject
        ).issuer_name(
            issuer_cert.subject
        ).public_key(
            public_key
        ).serial_number(
            serial
        ).not_valid_before(
            datetime.datetime.utcnow()
        ).not_valid_after(
            datetime.datetime.utcnow() + datetime.timedelta(days=valid_days)
        ).add_extension(
            x509.BasicConstraints(ca=False, path_length=None), critical=True
        ).add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=True,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False, 
                encipher_only=False,
                decipher_only=False
            ), critical=True
        ).add_extension(
            x509.ExtendedKeyUsage([
                ExtendedKeyUsageOID.SERVER_AUTH,  # Primary purpose is server authentication
                ExtendedKeyUsageOID.CLIENT_AUTH,  # Optionally include client auth for mutual TLS
            ]), critical=True
        ).add_extension(
            x509.SubjectKeyIdentifier.from_public_key(public_key), critical=False
        ).add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(issuer_key.public_key()),
            critical=False
        )
        
        # Add subject alternative names for domains
        if domains:
            san_objects = [x509.DNSName(domain) for domain in domains]
            cert_builder = cert_builder.add_extension(
                x509.SubjectAlternativeName(san_objects),
                critical=False
            )
        
        # Sign the certificate with the issuer's private key
        certificate = cert_builder.sign(
            private_key=issuer_key, 
            algorithm=hashes.SHA256()
        )
        
        return certificate, private_key
    
    def _build_name(self, attributes):
        """Build an X.509 name from attributes dictionary."""
        name_attributes = []
        
        # Map common dictionary keys to OIDs
        oid_map = {
            'common_name': NameOID.COMMON_NAME,
            'country': NameOID.COUNTRY_NAME,
            'state': NameOID.STATE_OR_PROVINCE_NAME,
            'locality': NameOID.LOCALITY_NAME,
            'organization': NameOID.ORGANIZATION_NAME,
            'organizational_unit': NameOID.ORGANIZATIONAL_UNIT_NAME,
            'email': NameOID.EMAIL_ADDRESS,
            'serial_number': NameOID.SERIAL_NUMBER
        }
        
        # If attributes is empty or doesn't contain common_name, add a default one
        if not attributes or 'common_name' not in attributes:
            default_cn = f"Certificate-{uuid.uuid4().hex[:8]}"
            name_attributes.append(x509.NameAttribute(NameOID.COMMON_NAME, default_cn))
        
        # Add all valid attributes from the dictionary
        for key, value in attributes.items():
            if key in oid_map and value:
                name_attributes.append(x509.NameAttribute(oid_map[key], value))
        
        # If still empty (unlikely after default CN), add a truly minimal name
        if not name_attributes:
            name_attributes.append(x509.NameAttribute(NameOID.COMMON_NAME, "Default-Certificate"))
                
        return x509.Name(name_attributes)
    
    @staticmethod
    def save_private_key(private_key, filename, password=None, encoding='PEM'):
        """Save private key to file with optional password protection."""
        enc = getattr(Encoding, encoding.upper())
        
        if password:
            encryption = serialization.BestAvailableEncryption(password.encode())
        else:
            encryption = serialization.NoEncryption()
            
        with open(filename, 'wb') as f:
            f.write(private_key.private_bytes(
                encoding=enc,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=encryption
            ))
    
    @staticmethod
    def save_certificate(cert, filename, encoding='PEM'):
        """Save certificate to file."""
        enc = getattr(Encoding, encoding.upper())
        
        with open(filename, 'wb') as f:
            f.write(cert.public_bytes(enc))
    
    @staticmethod
    def load_private_key(filename, password=None):
        """Load private key from file."""
        with open(filename, 'rb') as f:
            key_data = f.read()
            
        if password:
            return serialization.load_pem_private_key(
                key_data, 
                password=password.encode()
            )
        else:
            return serialization.load_pem_private_key(
                key_data,
                password=None
            )
    
    @staticmethod
    def load_certificate(filename):
        """Load certificate from file."""
        with open(filename, 'rb') as f:
            cert_data = f.read()
            
        return x509.load_pem_x509_certificate(cert_data)