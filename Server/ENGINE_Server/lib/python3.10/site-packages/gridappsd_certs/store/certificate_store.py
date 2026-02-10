"""
Certificate storage functionality for managing X.509 certificates.
"""

import os
import json
import base64
from pathlib import Path
from typing import Dict, List, Optional, Union, Set, Tuple
from datetime import datetime
import logging

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, ec
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID

from .ieee2030_5 import calculate_lfdi_from_certificate, calculate_sfdi_from_lfdi

logger = logging.getLogger("gridappsd.certs.store")


class CertificateStore:
    """
    Store for X.509 certificates with lookup capabilities.
    
    This class provides methods to:
    - Add certificates to the store
    - Look up certificates by various attributes
    - Store certificates in a filesystem or database backend
    - Load certificates from storage
    """
    
    def __init__(self, storage_path: Optional[str] = None):
        """
        Initialize the certificate store.
        
        Args:
            storage_path: Path to directory for certificate storage
                If None, certificates are kept in memory only
        """
        self.storage_path = Path(storage_path) if storage_path else None
        self.certificates: Dict[str, x509.Certificate] = {}  # By fingerprint
        self.private_keys: Dict[str, Union[rsa.RSAPrivateKey, ec.EllipticCurvePrivateKey]] = {}
        self.common_name_index: Dict[str, Set[str]] = {}  # CN -> set of fingerprints
        self.subject_alt_name_index: Dict[str, str] = {}  # SAN value -> fingerprint
        self.lfdi_index: Dict[str, str] = {}  # LFDI -> fingerprint
        self.sfdi_index: Dict[str, str] = {}  # SFDI -> fingerprint
        
        # Load certificates if storage path exists
        if self.storage_path and self.storage_path.exists():
            self.load_certificates()
    
    def add_certificate(
        self, 
        certificate: x509.Certificate,
        private_key: Optional[Union[rsa.RSAPrivateKey, ec.EllipticCurvePrivateKey]] = None,
        alias: Optional[str] = None
    ) -> str:
        """
        Add a certificate to the store.
        
        Args:
            certificate: X.509 certificate to add
            private_key: Optional private key for the certificate
            alias: Optional friendly name for the certificate
            
        Returns:
            Fingerprint of the certificate (SHA-256)
        """
        # Calculate fingerprint
        fingerprint = self._get_fingerprint(certificate)
        
        # Store certificate
        self.certificates[fingerprint] = certificate
        
        # Store private key if provided
        if private_key:
            self.private_keys[fingerprint] = private_key
        
        # Index by common name
        common_name = self._get_common_name(certificate)
        if common_name:
            if common_name not in self.common_name_index:
                self.common_name_index[common_name] = set()
            self.common_name_index[common_name].add(fingerprint)
        
        # Index by subject alternative names
        for san in self._get_subject_alt_names(certificate):
            self.subject_alt_name_index[san] = fingerprint

        try:
            lfdi = calculate_lfdi_from_certificate(certificate)
            if lfdi:
                self.lfdi_index[lfdi] = fingerprint
                
                # Also index by SFDI
                sfdi = calculate_sfdi_from_lfdi(lfdi)
                if sfdi:
                    self.sfdi_index[sfdi] = fingerprint
        except Exception as e:
            logger.warning(f"Failed to calculate LFDI/SFDI for certificate: {e}")
        
        # Persist to storage if enabled
        if self.storage_path:
            self._save_certificate(certificate, fingerprint, private_key, alias)
        
        logger.info(f"Added certificate with fingerprint {fingerprint} to store")
        return fingerprint
    
    def get_certificate_by_lfdi(self, lfdi: str) -> Optional[x509.Certificate]:
        """
        Get a certificate by Long Form Device Identifier (LFDI).
        
        Args:
            lfdi: LFDI to search for
            
        Returns:
            Certificate if found, None otherwise
        """
        fingerprint = self.lfdi_index.get(lfdi)
        if fingerprint:
            return self.certificates.get(fingerprint)
        return None
    
    def get_certificate_by_sfdi(self, sfdi: str) -> Optional[x509.Certificate]:
        """
        Get a certificate by Short Form Device Identifier (SFDI).
        
        Args:
            sfdi: SFDI to search for
            
        Returns:
            Certificate if found, None otherwise
        """
        fingerprint = self.sfdi_index.get(sfdi)
        if fingerprint:
            return self.certificates.get(fingerprint)
        return None
    
    def get_certificate_by_fingerprint(self, fingerprint: str) -> Optional[x509.Certificate]:
        """
        Get a certificate by its fingerprint.
        
        Args:
            fingerprint: SHA-256 fingerprint of the certificate
            
        Returns:
            Certificate if found, None otherwise
        """
        return self.certificates.get(fingerprint)
    
    def get_certificate_by_common_name(self, common_name: str) -> List[x509.Certificate]:
        """
        Get certificates by common name.
        
        Args:
            common_name: Common Name (CN) to search for
            
        Returns:
            List of matching certificates (may be empty)
        """
        fingerprints = self.common_name_index.get(common_name, set())
        return [self.certificates[fp] for fp in fingerprints if fp in self.certificates]
    
    def get_certificate_by_san(self, san_value: str) -> Optional[x509.Certificate]:
        """
        Get a certificate by Subject Alternative Name value.
        
        Args:
            san_value: SAN value to search for (e.g., email, DNS name, URI)
            
        Returns:
            Certificate if found, None otherwise
        """
        fingerprint = self.subject_alt_name_index.get(san_value)
        if fingerprint:
            return self.certificates.get(fingerprint)
        return None
    
    def get_private_key(self, certificate: Union[x509.Certificate, str]) -> Optional[Union[rsa.RSAPrivateKey, ec.EllipticCurvePrivateKey]]:
        """
        Get private key for a certificate.
        
        Args:
            certificate: Certificate or fingerprint
            
        Returns:
            Private key if found, None otherwise
        """
        if isinstance(certificate, x509.Certificate):
            fingerprint = self._get_fingerprint(certificate)
        else:
            fingerprint = certificate
        
        return self.private_keys.get(fingerprint)
    
    def list_certificates(self) -> List[Tuple[str, str, int, int]]:
        """
        List all certificates in the store.
        
        Returns:
            List of tuples (fingerprint, subject, not_before, not_after)
            where times are seconds since the Unix epoch
        """
        result = []
        for fingerprint, cert in self.certificates.items():
            subject = self._format_subject(cert.subject)
            result.append((
                fingerprint,
                subject,
                int(cert.not_valid_before.timestamp()),  # Convert to IEEE 2030.5 time
                int(cert.not_valid_after.timestamp())    # Convert to IEEE 2030.5 time
            ))
        return result
    
    def load_certificates(self):
        """Load certificates from storage."""
        if not self.storage_path or not self.storage_path.exists():
            return
        
        # Load certificate index
        index_path = self.storage_path / "index.json"
        if not index_path.exists():
            return
        
        try:
            with open(index_path, 'r') as f:
                index = json.load(f)
            
            for entry in index.get('certificates', []):
                fingerprint = entry['fingerprint']
                cert_path = self.storage_path / entry['cert_file']
                
                if cert_path.exists():
                    with open(cert_path, 'rb') as f:
                        cert_data = f.read()
                        cert = x509.load_pem_x509_certificate(cert_data)
                        self.certificates[fingerprint] = cert
                        
                        # Index by common name
                        common_name = self._get_common_name(cert)
                        if common_name:
                            if common_name not in self.common_name_index:
                                self.common_name_index[common_name] = set()
                            self.common_name_index[common_name].add(fingerprint)
                        
                        # Index by subject alternative names
                        for san in self._get_subject_alt_names(cert):
                            self.subject_alt_name_index[san] = fingerprint
                
                # Load private key if exists
                if 'key_file' in entry:
                    key_path = self.storage_path / entry['key_file']
                    if key_path.exists():
                        with open(key_path, 'rb') as f:
                            key_data = f.read()
                            if entry.get('key_encrypted', False):
                                # You'd need a password callback here for encrypted keys
                                continue
                            private_key = serialization.load_pem_private_key(
                                key_data,
                                password=None
                            )
                            self.private_keys[fingerprint] = private_key
            
            logger.info(f"Loaded {len(self.certificates)} certificates from storage")
            
        except Exception as e:
            logger.error(f"Error loading certificates: {e}")

        # Populate LFDI/SFDI indices
        for fingerprint, cert in self.certificates.items():
            try:
                lfdi = calculate_lfdi_from_certificate(cert)
                if lfdi:
                    self.lfdi_index[lfdi] = fingerprint
                    
                    # Also index by SFDI
                    sfdi = calculate_sfdi_from_lfdi(lfdi)
                    if sfdi:
                        self.sfdi_index[sfdi] = fingerprint
            except Exception as e:
                logger.warning(f"Failed to calculate LFDI/SFDI for certificate {fingerprint}: {e}")
    
    def _save_certificate(
        self,
        certificate: x509.Certificate,
        fingerprint: str,
        private_key: Optional[Union[rsa.RSAPrivateKey, ec.EllipticCurvePrivateKey]] = None,
        alias: Optional[str] = None
    ):
        """Save a certificate to storage based on its type."""
        if not self.storage_path:
            return
        
        # Create storage directory if it doesn't exist
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        # Determine certificate type and appropriate filename prefix
        cert_type, identifier = self._determine_certificate_type(certificate, alias)
        
        # Generate filenames based on type
        if cert_type == "device":
            prefix = f"device_{identifier}"
        elif cert_type == "ca":
            prefix = "ca"
        elif cert_type == "server":
            # Use domain name for server certificates
            domain = identifier.replace("*.", "wildcard_")  # Handle wildcards in domain
            prefix = f"server_{domain}"
        else:  # fallback to fingerprint
            safe_fingerprint = fingerprint.replace(':', '_')
            prefix = safe_fingerprint
        
        # The rest of the method remains the same...
        cert_filename = f"{prefix}.cert.pem"
        key_filename = f"{prefix}.key.pem" if private_key else None
        
        # Save certificate
        cert_path = self.storage_path / cert_filename
        with open(cert_path, 'wb') as f:
            f.write(certificate.public_bytes(serialization.Encoding.PEM))
        
        # Save private key if provided
        if private_key and key_filename:
            key_path = self.storage_path / key_filename
            with open(key_path, 'wb') as f:
                f.write(private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                ))
        
        # Update or create index file
        index_path = self.storage_path / "index.json"
        index = {'certificates': []}
        if index_path.exists():
            try:
                with open(index_path, 'r') as f:
                    index = json.load(f)
            except json.JSONDecodeError:
                pass
        
        # Find existing entry or create new one
        entry = None
        for e in index.get('certificates', []):
            if e['fingerprint'] == fingerprint:
                entry = e
                break
        
        if not entry:
            entry = {'fingerprint': fingerprint}
            index.setdefault('certificates', []).append(entry)
        
        entry.update({
            'cert_file': cert_filename,
            'subject': self._format_subject(certificate.subject),
            'not_before': int(certificate.not_valid_before.timestamp()),
            'not_after': int(certificate.not_valid_after.timestamp()),
            'alias': alias,
            'cert_type': cert_type,
            'identifier': identifier
        })
        
        if key_filename:
            entry.update({
                'key_file': key_filename,
                'key_encrypted': False
            })
        
        with open(index_path, 'w') as f:
            json.dump(index, f, indent=2)
    
    def _get_fingerprint(self, certificate: x509.Certificate) -> str:
        """Calculate SHA-256 fingerprint of a certificate."""
        fingerprint = certificate.fingerprint(hashes.SHA256())
        return fingerprint.hex(':')
    
    def _get_common_name(self, certificate: x509.Certificate) -> Optional[str]:
        """Extract Common Name from certificate subject."""
        for attr in certificate.subject:
            if attr.oid == NameOID.COMMON_NAME:
                return attr.value
        return None
    
    def _get_subject_alt_names(self, certificate: x509.Certificate) -> List[str]:
        """Extract Subject Alternative Name values."""
        result = []
        try:
            san_ext = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName)
            for name in san_ext.value:
                if isinstance(name, x509.DNSName):
                    result.append(f"DNS:{name.value}")
                elif isinstance(name, x509.RFC822Name):
                    result.append(f"EMAIL:{name.value}")
                elif isinstance(name, x509.UniformResourceIdentifier):
                    result.append(f"URI:{name.value}")
                # Add other SAN types as needed
        except x509.extensions.ExtensionNotFound:
            pass
        
        return result
    
    def _format_subject(self, subject: x509.Name) -> str:
        """Format certificate subject as string."""
        parts = []
        for attr in subject:
            oid_name = attr.oid._name
            parts.append(f"{oid_name}={attr.value}")
        return ", ".join(parts)
    
    def _determine_certificate_type(self, certificate: x509.Certificate, alias: Optional[str] = None) -> Tuple[str, str]:
        """
        Determine the type of certificate (device, CA, or server) and its identifier.
        
        Args:
            certificate: The X.509 certificate to analyze
            alias: Optional alias that might contain type information
            
        Returns:
            Tuple of (cert_type, identifier) where:
            - cert_type is one of "device", "ca", "server", or "unknown"
            - identifier is a unique identifier (e.g., device ID, domain name) or empty string
        """
        # Check if it's a CA certificate
        try:
            basic_constraints = certificate.extensions.get_extension_for_class(x509.BasicConstraints)
            if basic_constraints.value.ca:
                # It's a CA certificate
                common_name = self._get_common_name(certificate) or "root"
                return "ca", common_name
        except x509.extensions.ExtensionNotFound:
            pass
        
        # Check for device identifier in Subject Alternative Name
        try:
            san_ext = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName)
            
            # For server certificates, look for DNS names
            dns_names = [name.value for name in san_ext.value if isinstance(name, x509.DNSName)]
            if dns_names:
                # Check for server auth EKU
                try:
                    ext_key_usage = certificate.extensions.get_extension_for_class(x509.ExtendedKeyUsage)
                    if ExtendedKeyUsageOID.SERVER_AUTH in ext_key_usage.value:
                        # Server certificate with DNS names - use primary domain
                        return "server", dns_names[0]
                except x509.extensions.ExtensionNotFound:
                    pass
            
            # For device certificates, look for URIs with UUID
            for san in san_ext.value:
                if isinstance(san, x509.UniformResourceIdentifier):
                    uri = san.value
                    if uri.startswith("urn:uuid:"):
                        # This is likely a device certificate with a UUID-based identifier
                        device_id = uri.replace("urn:uuid:", "")
                        return "device", device_id
        except x509.extensions.ExtensionNotFound:
            pass
        
        # Check if alias provides type information
        if alias:
            if alias.lower() == "ca" or "ca" in alias.lower():
                common_name = self._get_common_name(certificate) or "root"
                return "ca", common_name
            elif "device" in alias.lower():
                # Extract device ID from alias if possible
                if "_" in alias:
                    parts = alias.split("_", 1)
                    if len(parts) > 1:
                        return "device", parts[1]
                return "device", alias
            elif "server" in alias.lower() or "web" in alias.lower():
                # For server alias, try to get the domain from Common Name
                common_name = self._get_common_name(certificate)
                if common_name and ("." in common_name):  # Simple check for a domain name
                    return "server", common_name
        
        # Use Common Name for identifying certificate type/ID
        common_name = self._get_common_name(certificate)
        if common_name:
            # Check if CN looks like a domain name for server certificates
            if "." in common_name and len(common_name.split(".")) > 1:
                # Verify if it has SERVER_AUTH EKU
                try:
                    ext_key_usage = certificate.extensions.get_extension_for_class(x509.ExtendedKeyUsage)
                    if ExtendedKeyUsageOID.SERVER_AUTH in ext_key_usage.value:
                        return "server", common_name
                except x509.extensions.ExtensionNotFound:
                    pass
            
            # If CN looks like a device ID or has CLIENT_AUTH only
            if common_name.isdigit() or "device" in common_name.lower():
                return "device", common_name
        
        # Check for CLIENT_AUTH without SERVER_AUTH
        try:
            ext_key_usage = certificate.extensions.get_extension_for_class(x509.ExtendedKeyUsage)
            if ExtendedKeyUsageOID.CLIENT_AUTH in ext_key_usage.value:
                if ExtendedKeyUsageOID.SERVER_AUTH not in ext_key_usage.value:
                    # It's a client-only certificate, likely a device
                    # Try to find a suitable identifier
                    if device_id := self._extract_device_id_from_certificate(certificate):
                        return "device", device_id
        except x509.extensions.ExtensionNotFound:
            pass
        
        # Check if we can derive an LFDI (which would indicate a device certificate)
        try:
            lfdi = calculate_lfdi_from_certificate(certificate)
            if lfdi:
                return "device", lfdi
        except Exception:
            pass
        
        # If we can't determine the type, use fingerprint as fallback
        fingerprint = self._get_fingerprint(certificate)
        return "unknown", fingerprint[:8]  # Use first 8 characters of fingerprint

    def _extract_device_id_from_certificate(self, certificate: x509.Certificate) -> Optional[str]:
        """Extract a device ID from certificate attributes."""
        # Try to get serial number from subject
        for attr in certificate.subject:
            if attr.oid == NameOID.SERIAL_NUMBER:
                return attr.value
        
        # Try common name if it looks like an ID
        common_name = self._get_common_name(certificate)
        if common_name and (common_name.startswith("Device") or common_name.isdigit()):
            return common_name
        
        # If device_id was passed in your test case
        try:
            sans = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
            for san in sans:
                if isinstance(san, x509.UniformResourceIdentifier) and "uuid" in san.value:
                    return san.value.split(":")[-1]
        except x509.extensions.ExtensionNotFound:
            pass
        
        return None
    
    def _extract_uri_device_id(self, certificate: x509.Certificate) -> Optional[str]:
        """Extract device ID from URI Subject Alternative Name."""
        try:
            san_ext = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName)
            for san in san_ext.value:
                if isinstance(san, x509.UniformResourceIdentifier):
                    uri = san.value
                    if uri.startswith("urn:uuid:"):
                        return uri.replace("urn:uuid:", "")
        except x509.extensions.ExtensionNotFound:
            pass
        return None