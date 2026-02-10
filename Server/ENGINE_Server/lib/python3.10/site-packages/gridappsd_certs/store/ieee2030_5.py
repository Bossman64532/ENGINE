"""
IEEE 2030.5 specific identity utilities.
"""

import hashlib
import binascii
import uuid
from typing import Optional, Dict, Tuple
from cryptography import x509
from cryptography.hazmat.primitives import serialization

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

def extract_device_information_from_certificate(certificate: x509.Certificate) -> Dict[str, str]:
    """
    Extract IEEE 2030.5 device information from a certificate.
    
    Args:
        certificate: X.509 certificate
        
    Returns:
        Dictionary with device information
    """
    device_info = {
        'lfdi': calculate_lfdi_from_certificate(certificate),
        'device_id': None,  # Will be populated below
        'pin_code': None    # Optional, may be included in SAN
    }
    
    # Calculate SFDI from LFDI
    device_info['sfdi'] = calculate_sfdi_from_lfdi(device_info['lfdi'])
    
    # Extract device ID from SAN (IEEE 2030.5 typically uses URN:UUID format)
    try:
        san_ext = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName)
        for name in san_ext.value:
            if isinstance(name, x509.UniformResourceIdentifier):
                if name.value.startswith('urn:uuid:'):
                    device_info['device_id'] = name.value[9:]  # Extract UUID part
                    break
    except x509.extensions.ExtensionNotFound:
        pass
    
    # If no device ID found in URI, create one based on LFDI
    if not device_info['device_id']:
        # Generate a deterministic UUID from LFDI
        namespace = uuid.UUID('00000000-0000-0000-0000-000000000000')
        device_info['device_id'] = str(uuid.uuid5(namespace, device_info['lfdi']))
    
    return device_info

def validate_pin_code(certificate: x509.Certificate, pin_code: str) -> bool:
    """
    Validate a PIN code against a certificate (for out-of-band validation).
    
    IEEE 2030.5 may use PIN codes for initial device configuration.
    This can be implemented according to your specific requirements.
    
    Args:
        certificate: X.509 certificate
        pin_code: PIN code to validate
        
    Returns:
        True if PIN code is valid, False otherwise
    """
    # Extract PIN from certificate or device profile
    # This is implementation-specific, as IEEE 2030.5 doesn't specify
    # exactly how PIN codes should be stored or validated
    
    # Example: Check if PIN matches last 8 chars of LFDI
    lfdi = calculate_lfdi_from_certificate(certificate)
    expected_pin = lfdi[-8:]  # Last 8 characters of LFDI
    
    return pin_code == expected_pin

def format_lfdi(lfdi: str, with_colons: bool = False) -> str:
    """
    Format LFDI for display.
    
    Args:
        lfdi: LFDI string
        with_colons: Whether to include colons between bytes
        
    Returns:
        Formatted LFDI string
    """
    if with_colons:
        return ':'.join(lfdi[i:i+2] for i in range(0, len(lfdi), 2))
    return lfdi

def format_sfdi(sfdi: str, with_dashes: bool = False) -> str:
    """
    Format SFDI for display.
    
    Args:
        sfdi: SFDI string
        with_dashes: Whether to include dashes for readability
        
    Returns:
        Formatted SFDI string
    """
    if with_dashes and len(sfdi) > 4:
        # Insert dashes every 4 digits for readability
        parts = []
        for i in range(0, len(sfdi), 4):
            parts.append(sfdi[i:i+4])
        return '-'.join(parts)
    return sfdi