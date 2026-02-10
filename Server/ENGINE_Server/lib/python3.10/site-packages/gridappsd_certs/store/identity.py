"""
Identity extraction from certificates for IEEE 2030.5 server.
"""

import hashlib
import binascii
from typing import Optional, Dict, Any, List, Tuple
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.x509.oid import NameOID

def extract_identity_from_certificate(certificate: x509.Certificate) -> Dict[str, Any]:
    """
    Extract identity information from a certificate.
    
    Args:
        certificate: X.509 certificate
        
    Returns:
        Dictionary with identity information
    """
    identity = {
        'common_name': None,
        'organization': None,
        'organizational_unit': None,
        'country': None,
        'email': None,
        'subject_alt_names': {
            'dns': [],
            'uri': [],
            'email': [],
            'ip': []
        },
        'serial_number': certificate.serial_number,
        'not_valid_before': certificate.not_valid_before,
        'not_valid_after': certificate.not_valid_after,
        'is_ca': False,
        # IEEE 2030.5 specific fields
        'lfdi': calculate_lfdi_from_certificate(certificate),
        'sfdi': None
    }
    
    # Calculate SFDI from LFDI
    if identity['lfdi']:
        identity['sfdi'] = calculate_sfdi_from_lfdi(identity['lfdi'])
    
    # Extract subject fields
    for attr in certificate.subject:
        if attr.oid == NameOID.COMMON_NAME:
            identity['common_name'] = attr.value
        elif attr.oid == NameOID.ORGANIZATION_NAME:
            identity['organization'] = attr.value
        elif attr.oid == NameOID.ORGANIZATIONAL_UNIT_NAME:
            identity['organizational_unit'] = attr.value
        elif attr.oid == NameOID.COUNTRY_NAME:
            identity['country'] = attr.value
        elif attr.oid == NameOID.EMAIL_ADDRESS:
            identity['email'] = attr.value
    
    # Extract Subject Alternative Names
    try:
        san_ext = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName)
        for name in san_ext.value:
            if isinstance(name, x509.DNSName):
                identity['subject_alt_names']['dns'].append(name.value)
            elif isinstance(name, x509.RFC822Name):
                identity['subject_alt_names']['email'].append(name.value)
            elif isinstance(name, x509.UniformResourceIdentifier):
                identity['subject_alt_names']['uri'].append(name.value)
            elif isinstance(name, x509.IPAddress):
                identity['subject_alt_names']['ip'].append(str(name.value))
    except x509.extensions.ExtensionNotFound:
        pass
    
    # Check if it's a CA certificate
    try:
        bc_ext = certificate.extensions.get_extension_for_class(x509.BasicConstraints)
        identity['is_ca'] = bc_ext.value.ca
    except x509.extensions.ExtensionNotFound:
        pass
    
    return identity

def extract_client_id_from_certificate(certificate: x509.Certificate) -> Optional[str]:
    """
    Extract a unique client ID from a certificate.
    
    For IEEE 2030.5, this is typically:
    1. URI from SAN with urn:uuid format
    2. Common Name
    3. LFDI
    4. Serial number
    """
    # First try to get URI from SAN
    try:
        san_ext = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName)
        for name in san_ext.value:
            if isinstance(name, x509.UniformResourceIdentifier):
                uri = name.value
                # Check if it's a IEEE 2030.5 URN UUID format
                if uri.startswith('urn:uuid:'):
                    return uri[9:]  # Return just the UUID part
                return uri
    except x509.extensions.ExtensionNotFound:
        pass
    
    # Try Common Name
    for attr in certificate.subject:
        if attr.oid == NameOID.COMMON_NAME:
            return attr.value
    
    # Try LFDI
    lfdi = calculate_lfdi_from_certificate(certificate)
    if lfdi:
        return f"lfdi:{lfdi}"
    
    # Use serial number as last resort
    return f"cert-{certificate.serial_number}"

def calculate_lfdi_from_certificate(certificate: x509.Certificate) -> str:
    """
    Calculate the Long Form Device Identifier (LFDI) from a certificate.
    
    IEEE 2030.5 specifies LFDI as a 20-byte (160-bit) identifier.
    Per the standard, it's typically the SHA-1 hash of the device's certificate.
    
    Args:
        certificate: X.509 certificate
        
    Returns:
        LFDI as a hexadecimal string
    """
    # Get DER encoding of certificate
    cert_bytes = certificate.public_bytes(encoding=serialization.Encoding.DER)
    
    # Calculate SHA-1 hash - as per IEEE 2030.5 section 8.5
    sha1_hash = hashlib.sha1(cert_bytes).digest()
    
    # Convert to hex string (40 characters)
    return binascii.hexlify(sha1_hash).decode('ascii').upper()

def calculate_sfdi_from_lfdi(lfdi: str) -> str:
    """
    Calculate the Short Form Device Identifier (SFDI) from LFDI.
    
    IEEE 2030.5 specifies SFDI as a 40-bit (5-byte) identifier derived from LFDI.
    Per the standard, it's typically the bottom 40 bits of the LFDI.
    
    Args:
        lfdi: LFDI as a hexadecimal string
        
    Returns:
        SFDI as a decimal string
    """
    # Convert LFDI from hex to bytes
    lfdi_bytes = binascii.unhexlify(lfdi)
    
    # Get the last 5 bytes (40 bits)
    sfdi_bytes = lfdi_bytes[-5:]
    
    # Convert to decimal string (without leading 0)
    sfdi_int = int.from_bytes(sfdi_bytes, byteorder='big')
    return str(sfdi_int)