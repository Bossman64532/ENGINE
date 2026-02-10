"""
Client registry with certificate-based identity and ACL support.
"""

from datetime import datetime
import json
from pathlib import Path
from typing import Dict, List, Optional, Set, Any, Union
import logging

from cryptography import x509

from .identity import extract_client_id_from_certificate, extract_identity_from_certificate
from .certificate_store import CertificateStore
from .ieee2030_5 import extract_device_information_from_certificate

logger = logging.getLogger("gridappsd.certs.registry")


class AccessControl:
    """Access control for resources."""
    
    def __init__(self):
        """Initialize empty ACL."""
        self._rules: Dict[str, Dict[str, bool]] = {}
    
    def add_rule(self, resource: str, method: str, allow: bool = True):
        """
        Add an access control rule.
        
        Args:
            resource: Resource path pattern
            method: HTTP method (GET, POST, etc.) or * for all
            allow: Whether to allow access
        """
        if resource not in self._rules:
            self._rules[resource] = {}
        
        self._rules[resource][method] = allow
    
    def check_access(self, resource: str, method: str) -> bool:
        """
        Check if access is allowed to a resource.
        
        Args:
            resource: Resource path to check
            method: HTTP method to check
            
        Returns:
            True if access is allowed, False otherwise
        """
        # Check for exact path match
        if resource in self._rules:
            # Check for exact method match
            if method in self._rules[resource]:
                return self._rules[resource][method]
            # Check for wildcard method
            if '*' in self._rules[resource]:
                return self._rules[resource]['*']
        
        # Check for pattern matches
        for pattern, methods in self._rules.items():
            if pattern.endswith('*') and resource.startswith(pattern[:-1]):
                if method in methods:
                    return methods[method]
                if '*' in methods:
                    return methods['*']
        
        # Default deny
        return False
    
    def to_dict(self) -> Dict[str, Dict[str, bool]]:
        """Convert ACL to dictionary."""
        return {r: dict(m) for r, m in self._rules.items()}
    
    @classmethod
    def from_dict(cls, data: Dict[str, Dict[str, bool]]) -> 'AccessControl':
        """Create ACL from dictionary."""
        acl = cls()
        for resource, methods in data.items():
            for method, allow in methods.items():
                acl.add_rule(resource, method, allow)
        return acl


class ClientProfile:
    """Client profile with identity and access control settings."""
    
    def __init__(
        self,
        client_id: str,
        certificate_fingerprint: Optional[str] = None,
        identity: Optional[Dict[str, Any]] = None,
        acl: Optional[Union[Dict[str, Dict[str, bool]], AccessControl]] = None,
        device_info: Optional[Dict[str, str]] = None 
    ):
        """
        Initialize client profile.
        
        Args:
            client_id: Unique client identifier
            certificate_fingerprint: Fingerprint of client's certificate
            identity: Identity information
            acl: Access control list
            device_info: IEEE 2030.5 device information
        """
        self.client_id = client_id
        self.certificate_fingerprint = certificate_fingerprint
        self.identity = identity or {}
        self.device_info = device_info or {}
        
        if isinstance(acl, AccessControl):
            self.acl = acl
        else:
            self.acl = AccessControl.from_dict(acl or {})
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert profile to dictionary."""
        # Convert datetime objects to seconds since epoch (IEEE 2030.5 convention)
        def convert_to_2030_5_time(obj):
            if isinstance(obj, datetime):
                # Convert to seconds since epoch
                return int(obj.timestamp())
            elif isinstance(obj, dict):
                return {k: convert_to_2030_5_time(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_to_2030_5_time(i) for i in obj]
            return obj
        
        # Create the base dictionary
        result = {
            'client_id': self.client_id,
            'certificate_fingerprint': self.certificate_fingerprint,
            'identity': self.identity,
            'acl': self.acl.to_dict(),
            'device_info': self.device_info
        }
        
        # Convert datetime objects to IEEE 2030.5 timestamps
        return convert_to_2030_5_time(result)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ClientProfile':
        """Create profile from dictionary."""
        # IEEE 2030.5 uses integer timestamps, no need to convert back to datetime
        return cls(
            client_id=data['client_id'],
            certificate_fingerprint=data.get('certificate_fingerprint'),
            identity=data.get('identity'),
            acl=data.get('acl'),
            device_info=data.get('device_info')
        )
    
    def can_access(self, resource: str, method: str) -> bool:
        """Check if client can access a resource with a specific method."""
        return self.acl.check_access(resource, method)
    
    

class ClientRegistry:
    """
    Registry of clients with certificate-based identity and access control.
    """
    
    def __init__(self, cert_store: Optional[CertificateStore] = None):
        """
        Initialize client registry.
        
        Args:
            cert_store: Optional certificate store
        """
        self.clients: Dict[str, ClientProfile] = {}
        self.cert_store = cert_store or CertificateStore()
        self.cert_to_client: Dict[str, str] = {}  # cert fingerprint -> client_id
    
    def add_client(
        self,
        client_id: str,
        certificate: Optional[x509.Certificate] = None,
        profile: Optional[ClientProfile] = None
    ) -> ClientProfile:
        """
        Add or update a client in the registry.
        
        Args:
            client_id: Unique client identifier
            certificate: Client certificate
            profile: Client profile (if None, a new one will be created)
            
        Returns:
            Created or updated client profile
        """
        # Create profile if needed
        if not profile:
            profile = ClientProfile(client_id=client_id)
        
        # Add certificate if provided
        if certificate:
            fingerprint = self.cert_store._get_fingerprint(certificate)
            self.cert_store.add_certificate(certificate)
            
            # Map certificate to client
            self.cert_to_client[fingerprint] = client_id
            profile.certificate_fingerprint = fingerprint
            
            # Extract and store identity information
            profile.identity = extract_identity_from_certificate(certificate)
        
            # Extract IEEE 2030.5 device information
            profile.device_info = extract_device_information_from_certificate(certificate)

        # Store profile
        self.clients[client_id] = profile
        
        return profile
    
    def get_client_by_lfdi(self, lfdi: str) -> Optional[ClientProfile]:
        """
        Get client profile by LFDI.
        
        Args:
            lfdi: Long Form Device Identifier
            
        Returns:
            Client profile or None if not found
        """
        # First check cert store for the certificate
        cert = self.cert_store.get_certificate_by_lfdi(lfdi)
        if cert:
            fingerprint = self.cert_store._get_fingerprint(cert)
            client_id = self.cert_to_client.get(fingerprint)
            if client_id:
                return self.clients.get(client_id)
        
        # Try searching client profiles directly
        for client_id, profile in self.clients.items():
            if profile.device_info.get('lfdi') == lfdi:
                return profile
        
        return None
    
    def get_client_by_sfdi(self, sfdi: str) -> Optional[ClientProfile]:
        """
        Get client profile by SFDI.
        
        Args:
            sfdi: Short Form Device Identifier
            
        Returns:
            Client profile or None if not found
        """
        # First check cert store for the certificate
        cert = self.cert_store.get_certificate_by_sfdi(sfdi)
        if cert:
            fingerprint = self.cert_store._get_fingerprint(cert)
            client_id = self.cert_to_client.get(fingerprint)
            if client_id:
                return self.clients.get(client_id)
        
        # Try searching client profiles directly
        for client_id, profile in self.clients.items():
            if profile.device_info.get('sfdi') == sfdi:
                return profile
        
        return None
    
    def get_client_by_id(self, client_id: str) -> Optional[ClientProfile]:
        """
        Get client profile by ID.
        
        Args:
            client_id: Client ID to look up
            
        Returns:
            Client profile or None if not found
        """
        return self.clients.get(client_id)
    
    def get_client_by_certificate(self, certificate: x509.Certificate) -> Optional[ClientProfile]:
        """
        Get client profile associated with a certificate.
        
        Args:
            certificate: Client certificate
            
        Returns:
            Client profile or None if not found
        """
        fingerprint = self.cert_store._get_fingerprint(certificate)
        client_id = self.cert_to_client.get(fingerprint)
        if client_id:
            return self.clients.get(client_id)
        
        # Try extracting client ID from certificate
        extracted_id = extract_client_id_from_certificate(certificate)
        if extracted_id and extracted_id in self.clients:
            # Map certificate to client for future lookups
            self.cert_to_client[fingerprint] = extracted_id
            return self.clients.get(extracted_id)
        
        return None
    
    def get_client_by_cert_fingerprint(self, fingerprint: str) -> Optional[ClientProfile]:
        """
        Get client profile by certificate fingerprint.
        
        Args:
            fingerprint: Certificate fingerprint
            
        Returns:
            Client profile or None if not found
        """
        client_id = self.cert_to_client.get(fingerprint)
        if client_id:
            return self.clients.get(client_id)
        return None
    
    def list_clients(self) -> List[str]:
        """
        List all client IDs in the registry.
        
        Returns:
            List of client IDs
        """
        return list(self.clients.keys())
    
    def remove_client(self, client_id: str) -> bool:
        """
        Remove a client from the registry.
        
        Args:
            client_id: Client ID to remove
            
        Returns:
            True if client was removed, False otherwise
        """
        if client_id in self.clients:
            profile = self.clients[client_id]
            if profile.certificate_fingerprint:
                if profile.certificate_fingerprint in self.cert_to_client:
                    del self.cert_to_client[profile.certificate_fingerprint]
            
            del self.clients[client_id]
            return True
        
        return False
    
    def save(self, path: str) -> None:
        """
        Save registry to a file.
        
        Args:
            path: File path to save to
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        data = {
            'clients': {client_id: profile.to_dict() for client_id, profile in self.clients.items()},
            'cert_to_client': self.cert_to_client
        }
        
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
    
    @classmethod
    def load(cls, path: str, cert_store: Optional[CertificateStore] = None) -> 'ClientRegistry':
        """
        Load registry from a file.
        
        Args:
            path: File path to load from
            cert_store: Optional certificate store
            
        Returns:
            Loaded client registry
        """
        path = Path(path)
        if not path.exists():
            return cls(cert_store=cert_store)
        
        registry = cls(cert_store=cert_store)
        
        with open(path, 'r') as f:
            data = json.load(f)
        
        # Load client profiles
        for client_id, profile_data in data.get('clients', {}).items():
            profile = ClientProfile.from_dict(profile_data)
            registry.clients[client_id] = profile
        
        # Load certificate mappings
        registry.cert_to_client = data.get('cert_to_client', {})
        
        return registry