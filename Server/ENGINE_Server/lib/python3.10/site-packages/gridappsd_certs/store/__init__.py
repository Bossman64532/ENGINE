"""
Certificate store functionality for IEEE 2030.5 servers.

This package provides tools to:
- Store and manage certificates
- Extract identity information from certificates
- Maintain a client registry with access control
- Validate certificates for authentication
- Support IEEE 2030.5 device identifiers (LFDI/SFDI)
"""

from .certificate_store import CertificateStore
from .identity import extract_identity_from_certificate, extract_client_id_from_certificate
from .registry import ClientRegistry, ClientProfile, AccessControl
from .validation import CertificateValidator, ValidationResult
from .ieee2030_5 import (
    calculate_lfdi_from_certificate,
    calculate_sfdi_from_lfdi,
    extract_device_information_from_certificate,
    validate_pin_code,
    format_lfdi,
    format_sfdi
)

__all__ = [
    'CertificateStore',
    'extract_identity_from_certificate',
    'extract_client_id_from_certificate',
    'ClientRegistry',
    'ClientProfile',
    'AccessControl',
    'CertificateValidator',
    'ValidationResult',
    'calculate_lfdi_from_certificate',
    'calculate_sfdi_from_lfdi',
    'extract_device_information_from_certificate',
    'validate_pin_code',
    'format_lfdi',
    'format_sfdi'
]