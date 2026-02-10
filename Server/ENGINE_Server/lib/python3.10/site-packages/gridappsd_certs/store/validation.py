"""
Certificate validation utilities for IEEE 2030.5 server.
"""

from typing import Optional, List, Dict, Any
import logging
from datetime import datetime

from cryptography import x509
from cryptography.x509.oid import ExtendedKeyUsageOID
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric import rsa, ec
from cryptography.exceptions import InvalidSignature

logger = logging.getLogger("gridappsd.certs.validation")


class ValidationResult:
    """Result of certificate validation."""
    
    def __init__(self, valid: bool = True, errors: Optional[List[str]] = None):
        """
        Initialize validation result.
        
        Args:
            valid: Whether certificate is valid
            errors: List of validation errors (if any)
        """
        self.valid = valid
        self.errors = errors or []
    
    def add_error(self, error: str):
        """Add an error to the validation result."""
        self.errors.append(error)
        self.valid = False
    
    def __bool__(self):
        """Allow using result in boolean context."""
        return self.valid


class CertificateValidator:
    """
    Validator for IEEE 2030.5 client certificates.
    
    Performs validation checks including:
    - Certificate expiration
    - Certificate chain validation
    - Key usage validation
    - Revocation status (if enabled)
    """
    
    def __init__(self, trust_store: Optional[List[x509.Certificate]] = None):
        """
        Initialize certificate validator.
        
        Args:
            trust_store: List of trusted CA certificates
        """
        self.trust_store = trust_store or []
    
    def validate(self, certificate: x509.Certificate) -> ValidationResult:
        """
        Validate a certificate.
        
        Args:
            certificate: Certificate to validate
            
        Returns:
            ValidationResult object
        """
        result = ValidationResult()
        
        # Check expiration
        now = datetime.utcnow()
        if certificate.not_valid_before > now:
            result.add_error("Certificate is not yet valid")
        if certificate.not_valid_after < now:
            result.add_error("Certificate has expired")
        
        # Check key usage
        self._validate_key_usage(certificate, result)
        
        # Check chain of trust if trust store is available
        if self.trust_store:
            self._validate_trust_chain(certificate, result)
        
        return result
    
    def _validate_key_usage(self, certificate: x509.Certificate, result: ValidationResult):
        """
        Validate key usage extensions.
        
        For IEEE 2030.5, certificates should have digitalSignature and keyEncipherment
        for client auth.
        
        Args:
            certificate: Certificate to validate
            result: ValidationResult to update
        """
        try:
            key_usage_ext = certificate.extensions.get_extension_for_class(x509.KeyUsage)
            key_usage = key_usage_ext.value
            
            if not key_usage.digital_signature:
                result.add_error("Certificate does not have digitalSignature key usage")
            
            if isinstance(certificate.public_key(), rsa.RSAPublicKey) and not key_usage.key_encipherment:
                result.add_error("RSA certificate does not have keyEncipherment key usage")
            
        except x509.extensions.ExtensionNotFound:
            result.add_error("Certificate does not have KeyUsage extension")
        
        # Check Extended Key Usage
        try:
            eku_ext = certificate.extensions.get_extension_for_class(x509.ExtendedKeyUsage)
            eku = eku_ext.value
            
            if ExtendedKeyUsageOID.CLIENT_AUTH not in eku:
                result.add_error("Certificate does not have clientAuth extended key usage")
            
        except x509.extensions.ExtensionNotFound:
            if key_usage_ext.critical:
                # If KeyUsage is critical but no EKU, that's acceptable
                pass
            else:
                result.add_error("Certificate does not have ExtendedKeyUsage extension")
    
    def _validate_trust_chain(self, certificate: x509.Certificate, result: ValidationResult):
        """
        Validate certificate against trust store.
        
        Args:
            certificate: Certificate to validate
            result: ValidationResult to update
        """
        # Simple validation: check if certificate is directly issued by a trusted CA
        issuer_name = certificate.issuer
        
        # First check if it's self-signed
        if certificate.subject == certificate.issuer:
            # Self-signed certificates need to be in the trust store
            for trusted_cert in self.trust_store:
                if certificate.subject == trusted_cert.subject:
                    try:
                        # Verify the self-signed certificate using its own public key
                        public_key = certificate.public_key()
                        if self._verify_signature(certificate, public_key):
                            return  # Valid self-signed certificate in trust store
                    except Exception as e:
                        logger.debug(f"Error verifying self-signed certificate: {e}")
            
            result.add_error("Self-signed certificate is not trusted")
            return
        
        # Check if issued by a trusted CA
        for trusted_cert in self.trust_store:
            if issuer_name == trusted_cert.subject:
                try:
                    # Verify the certificate using the CA's public key
                    ca_public_key = trusted_cert.public_key()
                    if self._verify_signature(certificate, ca_public_key):
                        return  # Valid certificate issued by trusted CA
                except Exception as e:
                    logger.debug(f"Error verifying certificate: {e}")
        
        result.add_error("Certificate is not issued by a trusted CA")
    
    def _verify_signature(self, certificate: x509.Certificate, public_key: Any) -> bool:
        """
        Verify certificate signature using public key.
        
        Args:
            certificate: Certificate to verify
            public_key: Public key to use for verification
            
        Returns:
            True if signature is valid, False otherwise
        """
        try:
            if isinstance(public_key, rsa.RSAPublicKey):
                public_key.verify(
                    certificate.signature,
                    certificate.tbs_certificate_bytes,
                    padding.PKCS1v15(),
                    certificate.signature_hash_algorithm
                )
            elif isinstance(public_key, ec.EllipticCurvePublicKey):
                public_key.verify(
                    certificate.signature,
                    certificate.tbs_certificate_bytes,
                    ec.ECDSA(certificate.signature_hash_algorithm)
                )
            else:
                return False
            
            return True
        except InvalidSignature:
            return False
        except Exception as e:
            logger.error(f"Error verifying signature: {e}")
            return False