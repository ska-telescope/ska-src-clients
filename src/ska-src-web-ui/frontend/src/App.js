import React, { useState, useEffect, useCallback, useRef } from 'react';
import axios from 'axios';
import './App.css';
import skaLogo from './skao-logo.png';

function App() {
  const [tokenRequest, setTokenRequest] = useState(null);
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState(null);
  const [tokens, setTokens] = useState([]);
  const [pollingInterval, setPollingInterval] = useState(null);
  const [serviceData, setServiceData] = useState(null);
  const [selectedService, setSelectedService] = useState(null);
  const [filters, setFilters] = useState({ site: '', serviceType: '', status: '' });
  const [collapsedSections, setCollapsedSections] = useState({ sites: false, services: false, compute: false, storage: false });
  const [loadingServiceData, setLoadingServiceData] = useState(false);
  const [storageData, setStorageData] = useState(null);
  const [loadingStorageData, setLoadingStorageData] = useState(false);
  const pollingIntervalRef = useRef(null);
  const [sitesList, setSitesList] = useState([]);
  const [selectedServiceIds, setSelectedServiceIds] = useState([]);
  // Add state to track which exchange button is animating
  const [clickedExchange, setClickedExchange] = useState(null);
  const [activeTokens, setActiveTokens] = useState({}); // { service_name: file_name }

  // API status monitoring
  const [apiStatus, setApiStatus] = useState({
    backend: { status: 'unknown', lastCheck: null, error: null },
    auth: { status: 'unknown', lastCheck: null, error: null },
    permissions: { status: 'unknown', lastCheck: null, error: null },
    'site-capabilities': { status: 'unknown', lastCheck: null, error: null },
    'data-management': { status: 'unknown', lastCheck: null, error: null },
    iam: { status: 'unknown', lastCheck: null, error: null },
    fts: { status: 'unknown', lastCheck: null, error: null },
    rucio: { status: 'unknown', lastCheck: null, error: null },
    gateway: { status: 'unknown', lastCheck: null, error: null },
    gatekeeper: { status: 'unknown', lastCheck: null, error: null },
    canfar: { status: 'unknown', lastCheck: null, error: null },
    soda: { status: 'unknown', lastCheck: null, error: null },
    'prepare-data': { status: 'unknown', lastCheck: null, error: null }
  });
  const [checkingApiStatus, setCheckingApiStatus] = useState(false);

  // API base URL
  const API_BASE = '/api/v1';

  // Available services for token exchange (based on the token's groups)
  const availableServices = [
    'data-management-api',
    'site-capabilities-api'
  ];

  // Request a new token
  const requestToken = async () => {
    // Check if core systems are online before starting token request
    if (!areCoreSystemsOnline()) {
      setStatus({ 
        type: 'error', 
        message: 'Cannot request token: Authentication, Permissions, or Backend is offline. Please check the system status panel above.' 
      });
      return;
    }
    
    setLoading(true);
    setStatus({ type: 'info', message: 'Starting token request...' });
    
    try {
      // Add timeout to the request
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 30000); // 30 second timeout
      
      const response = await axios.post(`${API_BASE}/auth/token/request`, {
        max_polling_attempts: 60,
        wait_between_polling_s: 5
      }, {
        signal: controller.signal,
        timeout: 30000 // 30 second timeout
      });
      
      clearTimeout(timeoutId);
      
      setTokenRequest(response.data);
      setStatus({ 
        type: 'success', 
        message: 'Token request initiated successfully! Please complete authentication in your browser.' 
      });
      
      // Start polling for completion
      startPolling(response.data.device_code);
      
    } catch (error) {
      console.error('Token request error:', error);
      
      let errorMessage = 'Failed to request token';
      if (error.response?.data?.detail) {
        errorMessage = `Failed to request token: ${error.response.data.detail}`;
      } else if (error.name === 'AbortError' || error.code === 'ECONNABORTED') {
        errorMessage = 'Token request timed out. The authentication server may be unavailable. Please try again later.';
      } else if (error.response?.status === 503) {
        errorMessage = 'Authentication server is currently unavailable. Please try again later.';
      } else {
        errorMessage = `${errorMessage}: ${error.message}`;
      }
      
      setStatus({ 
        type: 'error', 
        message: errorMessage
      });
    } finally {
      setLoading(false);
    }
  };

  // Start polling for token completion
  const startPolling = (deviceCode) => {
    // Prevent multiple intervals
    if (pollingIntervalRef.current) return;
    const poll = async () => {
      try {
        // Check if Authentication is still online before polling
        if (apiStatus.auth.status === 'offline') {
          console.log('Authentication went offline, stopping polling');
          setStatus({ 
            type: 'error', 
            message: 'Authentication service went offline. Token request cancelled. Please try again when the service is back online.' 
          });
          stopPolling();
          setTokenRequest(null); // Clear the device flow info
          return;
        }

        const response = await axios.get(`${API_BASE}/auth/token/check/${deviceCode}`, {
          timeout: 10000 // 10 second timeout for each poll
        });
        
        console.log('Polling response:', response.data);
        
        if (response.data.success) {
          console.log('Authentication successful, stopping polling');
          setStatus({ 
            type: 'success', 
            message: 'Authentication completed successfully! Token has been obtained.' 
          });
          stopPolling();
          loadTokens(); // Refresh token list
          setTokenRequest(null); // Optionally clear the device flow info
        } else if (response.data.fatal) {
          console.log('Fatal error, stopping polling');
          // Stop polling on fatal errors
          setStatus({ 
            type: 'error', 
            message: response.data.message 
          });
          stopPolling();
          setTokenRequest(null); // Clear the device flow info
        } else {
          console.log('Still pending, continuing to poll');
        }
        // If not success and not fatal, continue polling (pending state)
      } catch (error) {
        console.error('Polling error:', error);
        
        // Check if the error is due to Authentication being offline
        if (error.response?.status === 503 || error.code === 'ECONNREFUSED' || error.message.includes('Network Error')) {
          console.log('Network error during polling, checking if Authentication is offline');
          // Try to check Authentication status
          try {
            const authCheck = await axios.get(`${API_BASE}/auth/api-status`);
            if (authCheck.data.auth?.status === 'offline') {
              console.log('Authentication confirmed offline, stopping polling');
              setStatus({ 
                type: 'error', 
                message: 'Authentication service went offline. Token request cancelled. Please try again when the service is back online.' 
              });
              stopPolling();
              setTokenRequest(null);
              return;
            }
          } catch (authError) {
            console.log('Could not check Authentication status, continuing to poll');
          }
        }
        
        // Don't show error for other polling failures, just continue
      }
    };

    // Poll every 5 seconds
    const interval = setInterval(poll, 5000);
    pollingIntervalRef.current = interval;
    setPollingInterval(interval);
  };

  // Stop polling
  const stopPolling = useCallback(() => {
    console.log('stopPolling called, pollingIntervalRef.current:', pollingIntervalRef.current);
    if (pollingIntervalRef.current) {
      clearInterval(pollingIntervalRef.current);
      pollingIntervalRef.current = null;
      setPollingInterval(null);
      console.log('Polling stopped');
    }
  }, []);

  // Load existing tokens
  const loadTokens = async () => {
    try {
      setStatus({ type: 'info', message: 'Loading tokens...' });
      const response = await axios.get(`${API_BASE}/auth/tokens`, {
        timeout: 10000 // 10 second timeout
      });
      setTokens(response.data.tokens || []);
      if (response.data.tokens?.length > 0) {
        setStatus({ type: 'success', message: `Loaded ${response.data.tokens.length} tokens` });
      } else {
        setStatus({ type: 'info', message: 'No tokens found. Please request a new token to get started.' });
      }
    } catch (error) {
      console.error('Failed to load tokens:', error);
      let errorMessage = 'Failed to load tokens';
      if (error.code === 'ECONNABORTED') {
        errorMessage = 'Loading tokens timed out. Please try again.';
      } else if (error.response?.status === 503) {
        errorMessage = 'Authentication server is currently unavailable. Please try again later.';
      } else {
        errorMessage = `${errorMessage}: ${error.message}`;
      }
      setStatus({ type: 'error', message: errorMessage });
    }
  };

  // Delete a specific token by file name
  const deleteToken = async (fileName) => {
    try {
      setStatus({ type: 'info', message: `Deleting token file ${fileName}...` });
      const response = await axios.delete(`${API_BASE}/auth/tokens/by-file/${fileName}`, {
        timeout: 10000 // 10 second timeout
      });
      if (response.data.success) {
        setStatus({ type: 'success', message: `Token file ${fileName} deleted successfully` });
        // Refresh the token list
        loadTokens();
      } else {
        setStatus({ type: 'error', message: `Failed to delete token file ${fileName}` });
      }
    } catch (error) {
      console.error('Failed to delete token:', error);
      let errorMessage = 'Failed to delete token';
      if (error.code === 'ECONNABORTED') {
        errorMessage = 'Delete operation timed out. Please try again.';
      } else if (error.response?.status === 503) {
        errorMessage = 'Authentication server is currently unavailable. Please try again later.';
      } else if (error.response?.status === 404) {
        errorMessage = `Token file ${fileName} not found`;
      } else {
        errorMessage = `${errorMessage}: ${error.response?.data?.detail || error.message}`;
      }
      setStatus({ type: 'error', message: errorMessage });
    }
  };

  // Refresh a single token by file name
  const refreshToken = async (fileName) => {
    try {
      setStatus({ type: 'info', message: `Refreshing token file ${fileName}...` });
      const response = await axios.get(`${API_BASE}/auth/tokens/by-file/${fileName}`, {
        timeout: 10000 // 10 second timeout
      });
      if (response.data) {
        setTokens((prevTokens) => {
          // Replace the token with the same file_name, or add if not present
          const idx = prevTokens.findIndex(t => t.file_name === fileName);
          if (idx !== -1) {
            const newTokens = [...prevTokens];
            newTokens[idx] = response.data;
            return newTokens;
          } else {
            return [...prevTokens, response.data];
          }
        });
        setStatus({ type: 'success', message: `Token file ${fileName} refreshed` });
      } else {
        setStatus({ type: 'error', message: `Failed to refresh token file ${fileName}` });
      }
    } catch (error) {
      console.error('Failed to refresh token:', error);
      let errorMessage = 'Failed to refresh token';
      if (error.code === 'ECONNABORTED') {
        errorMessage = 'Refresh operation timed out. Please try again.';
      } else if (error.response?.status === 503) {
        errorMessage = 'Authentication server is currently unavailable. Please try again later.';
      } else if (error.response?.status === 404) {
        errorMessage = `Token file ${fileName} not found`;
      } else {
        errorMessage = `${errorMessage}: ${error.response?.data?.detail || error.message}`;
      }
      setStatus({ type: 'error', message: errorMessage });
    }
  };

  // Set active token for a service
  const setActiveToken = (serviceName, fileName) => {
    setActiveTokens(prev => ({
      ...prev,
      [serviceName]: fileName
    }));
  };

  // Check if core systems are online (required for any token exchange)
  const areCoreSystemsOnline = () => {
    return apiStatus.backend.status === 'online' &&
           apiStatus.auth.status === 'online' &&
           apiStatus.permissions.status === 'online';
  };

  // Check if site capabilities exchange is available
  const isSiteCapabilitiesExchangeAvailable = () => {
    return areCoreSystemsOnline() && 
           apiStatus['site-capabilities'].status === 'online';
  };

  // Check if data management exchange is available
  const isDataManagementExchangeAvailable = () => {
    return areCoreSystemsOnline() && 
           apiStatus['site-capabilities'].status === 'online' &&
           apiStatus['data-management'].status === 'online';
  };

  // Check if all systems are green (for display purposes)
  const areAllSystemsGreen = () => {
    return apiStatus.backend.status === 'online' &&
           apiStatus.auth.status === 'online' &&
           apiStatus.permissions.status === 'online' &&
           apiStatus['site-capabilities'].status === 'online' &&
           apiStatus['data-management'].status === 'online' &&
           apiStatus.iam.status === 'online' &&
           apiStatus.fts.status === 'online' &&
           apiStatus.rucio.status === 'online' &&
           apiStatus.gateway.status === 'online' &&
           apiStatus.gatekeeper.status === 'online' &&
           apiStatus.canfar.status === 'online' &&
           apiStatus.soda.status === 'online' &&
           apiStatus['prepare-data'].status === 'online';
  };

  // Exchange token for a service
  const exchangeToken = async (serviceName, version = 'latest') => {
    setClickedExchange(serviceName);
    setTimeout(() => setClickedExchange(null), 500); // Remove animation after 0.5s
    
    // Check if core systems are online (required for any token exchange)
    if (!areCoreSystemsOnline()) {
      setStatus({ 
        type: 'error', 
        message: 'Cannot exchange token: Authentication, Permissions, or Backend is offline. Please check the system status panel above.' 
      });
      return;
    }
    
    // Check specific service availability based on cascading logic
    if (serviceName === 'site-capabilities-api' && !isSiteCapabilitiesExchangeAvailable()) {
      setStatus({ 
        type: 'error', 
        message: 'Cannot exchange token for Site Capabilities: Site Capabilities service is offline. Please check the system status panel above.' 
      });
      return;
    }
    
    if (serviceName === 'data-management-api' && !isDataManagementExchangeAvailable()) {
      setStatus({ 
        type: 'error', 
        message: 'Cannot exchange token for Data Management: Data Management service is offline. Please check the system status panel above.' 
      });
      return;
    }
    
    try {
      setStatus({ type: 'info', message: `Exchanging token for ${serviceName}...` });
      const response = await axios.post(`${API_BASE}/auth/token/exchange`, {
        service_name: serviceName,
        version: version
      });
      
      if (response.data.success) {
        setStatus({ 
          type: 'success', 
          message: `Token exchanged successfully for ${serviceName}!` 
        });
        // Refresh tokens after successful exchange
        loadTokens();
      } else {
        setStatus({ 
          type: 'warning', 
          message: `Token exchange failed for ${serviceName}: ${response.data.message}` 
        });
      }
    } catch (error) {
      const errorMessage = error.response?.data?.detail || error.message;
      if (error.response?.status === 503) {
        setStatus({ 
          type: 'error', 
          message: 'Authentication server is currently unavailable. Please try again later.' 
        });
      } else {
      setStatus({ 
        type: 'error', 
        message: `Token exchange error for ${serviceName}: ${errorMessage}` 
      });
    }
    }
  };

  // Check API status for all services
  const checkApiStatus = async () => {
    setCheckingApiStatus(true);
    const now = new Date();
    
    try {
      // Check backend health first
      let backendStatus = { status: 'unknown', error: null };
    try {
      await axios.get(`${API_BASE}/auth/health`);
        backendStatus = { status: 'online', error: null };
    } catch (error) {
        backendStatus = { status: 'offline', error: error.message };
      }

      // Check external APIs
      let apiStatusData = {};
      try {
        const response = await axios.get(`${API_BASE}/auth/api-status`);
        apiStatusData = response.data;
      } catch (error) {
        console.error('Failed to check external API status:', error);
        apiStatusData = {
          auth: { status: 'unknown', error: 'Failed to check status' },
          permissions: { status: 'unknown', error: 'Failed to check status' },
          'site-capabilities': { status: 'unknown', error: 'Failed to check status' },
          'data-management': { status: 'unknown', error: 'Failed to check status' },
          iam: { status: 'unknown', error: 'Failed to check status' },
          fts: { status: 'unknown', error: 'Failed to check status' },
          rucio: { status: 'unknown', error: 'Failed to check status' },
          gateway: { status: 'unknown', error: 'Failed to check status' },
          gatekeeper: { status: 'unknown', error: 'Failed to check status' },
          canfar: { status: 'unknown', error: 'Failed to check status' },
          soda: { status: 'unknown', error: 'Failed to check status' },
          'prepare-data': { status: 'unknown', error: 'Failed to check status' }
        };
      }
      
      const newStatus = {
        backend: { 
          status: backendStatus.status, 
          lastCheck: now, 
          error: backendStatus.error 
        },
        auth: { 
          status: apiStatusData.auth?.status || 'unknown', 
          lastCheck: now, 
          error: apiStatusData.auth?.error 
        },
        permissions: { 
          status: apiStatusData.permissions?.status || 'unknown', 
          lastCheck: now, 
          error: apiStatusData.permissions?.error 
        },
        'site-capabilities': { 
          status: apiStatusData['site-capabilities']?.status || 'unknown', 
          lastCheck: now, 
          error: apiStatusData['site-capabilities']?.error 
        },
        'data-management': { 
          status: apiStatusData['data-management']?.status || 'unknown', 
          lastCheck: now, 
          error: apiStatusData['data-management']?.error 
        },
        iam: { 
          status: apiStatusData.iam?.status || 'unknown', 
          lastCheck: now, 
          error: apiStatusData.iam?.error 
        },
        fts: { 
          status: apiStatusData.fts?.status || 'unknown', 
          lastCheck: now, 
          error: apiStatusData.fts?.error 
        },
        rucio: { 
          status: apiStatusData.rucio?.status || 'unknown', 
          lastCheck: now, 
          error: apiStatusData.rucio?.error 
        },
        gateway: { 
          status: apiStatusData.gateway?.status || 'unknown', 
          lastCheck: now, 
          error: apiStatusData.gateway?.error 
        },
        gatekeeper: { 
          status: apiStatusData.gatekeeper?.status || 'unknown', 
          lastCheck: now, 
          error: apiStatusData.gatekeeper?.error 
        },
        canfar: { 
          status: apiStatusData.canfar?.status || 'unknown', 
          lastCheck: now, 
          error: apiStatusData.canfar?.error 
        },
        soda: { 
          status: apiStatusData.soda?.status || 'unknown', 
          lastCheck: now, 
          error: apiStatusData.soda?.error 
        },
        'prepare-data': { 
          status: apiStatusData['prepare-data']?.status || 'unknown', 
          lastCheck: now, 
          error: apiStatusData['prepare-data']?.error 
        }
      };
      
      console.log('Setting API status with timestamp:', now.toLocaleTimeString());
      setApiStatus(newStatus);
    } catch (error) {
      console.error('Failed to check API status:', error);
      // Set all APIs to unknown status if the check fails
      setApiStatus({
        backend: { status: 'unknown', lastCheck: now, error: 'Failed to check status' },
        auth: { status: 'unknown', lastCheck: now, error: 'Failed to check status' },
        permissions: { status: 'unknown', lastCheck: now, error: 'Failed to check status' },
        'site-capabilities': { status: 'unknown', lastCheck: now, error: 'Failed to check status' },
        'data-management': { status: 'unknown', lastCheck: now, error: 'Failed to check status' },
        iam: { status: 'unknown', lastCheck: now, error: 'Failed to check status' },
        fts: { status: 'unknown', lastCheck: now, error: 'Failed to check status' },
        rucio: { status: 'unknown', lastCheck: now, error: 'Failed to check status' },
        gateway: { status: 'unknown', lastCheck: now, error: 'Failed to check status' },
        gatekeeper: { status: 'unknown', lastCheck: now, error: 'Failed to check status' },
        canfar: { status: 'unknown', lastCheck: now, error: 'Failed to check status' },
        soda: { status: 'unknown', lastCheck: now, error: 'Failed to check status' },
        'prepare-data': { status: 'unknown', lastCheck: now, error: 'Failed to check status' }
      });
    } finally {
      setCheckingApiStatus(false);
    }
  };



  // Load service data for a specific service
  const loadServiceData = async (serviceName) => {
    try {
      setLoadingServiceData(true);
      setStatus({ type: 'info', message: `Loading ${serviceName} data...` });
      setSelectedService(serviceName);
      setFilters({ site: '', serviceType: '', status: '' }); // Reset filters
      
      let data = {};
      
      if (serviceName === 'data-management-api') {
        // Load data management functions
        const namespacesResponse = await axios.get(`${API_BASE}/auth/data/namespaces`);
        data.namespaces = namespacesResponse.data.data || [];
      } else if (serviceName === 'site-capabilities-api') {
        // Load site capabilities functions
        const servicesResponse = await axios.get(`${API_BASE}/auth/site/services`);
        const sitesResponse = await axios.get(`${API_BASE}/auth/site/sites`);
        const computeResponse = await axios.get(`${API_BASE}/auth/site/compute`);
        
        data.services = servicesResponse.data.data || [];
        data.sites = sitesResponse.data.data || [];
        data.compute = computeResponse.data.data || [];
      }
      
      setServiceData(data);
      setStatus({ type: 'success', message: `${serviceName} data loaded successfully` });
    } catch (error) {
      console.error(`Failed to load ${serviceName} data:`, error);
      
      // Handle specific error cases
      if (error.response?.status === 503) {
        setStatus({ 
          type: 'error', 
          message: 'Authentication server is currently unavailable. Please try again later.' 
        });
      } else if (error.response?.status === 401) {
        setStatus({ 
          type: 'warning', 
          message: 'Authentication required. Please request a token to access this data.' 
        });
      } else {
      setStatus({ type: 'error', message: `Failed to load ${serviceName} data: ${error.message}` });
      }
    } finally {
      setLoadingServiceData(false);
    }
  };

  // Load storage data
  const loadStorageData = async (siteName = null) => {
    try {
      setLoadingStorageData(true);
      setStatus({ type: 'info', message: 'Loading storage data...' });
      
      let url = `${API_BASE}/storage`;
      if (siteName) {
        url += `?parent_node_name=${encodeURIComponent(siteName)}`;
      }
      
      const response = await axios.get(url);
      setStorageData(response.data.data || []);
      setStatus({ type: 'success', message: `Loaded ${response.data.data?.length || 0} storage resources${siteName ? ` for ${siteName}` : ''}` });
    } catch (error) {
      console.error('Failed to load storage data:', error);
      if (error.response?.status === 503) {
        setStatus({ 
          type: 'error', 
          message: 'Authentication server is currently unavailable. Please try again later.' 
        });
      } else {
      setStatus({ type: 'error', message: `Failed to load storage data: ${error.message}` });
      }
    } finally {
      setLoadingStorageData(false);
    }
  };

  // Filter data based on selected filters
  const getFilteredData = (data, type) => {
    if (!data) return [];
    
    return data.filter(item => {
      const matchesSite = !filters.site || 
        (item.site && item.site.toLowerCase().includes(filters.site.toLowerCase())) ||
        (item.site_name && item.site_name.toLowerCase().includes(filters.site.toLowerCase())) ||
        (item.parent_site_name && item.parent_site_name.toLowerCase().includes(filters.site.toLowerCase())) ||
        (item.node && item.node.toLowerCase().includes(filters.site.toLowerCase())) ||
        (item.parent_node_name && item.parent_node_name.toLowerCase().includes(filters.site.toLowerCase()));
      
      const matchesType = !filters.serviceType || 
        (item.type && item.type.toLowerCase().includes(filters.serviceType.toLowerCase()));
      
      const matchesStatus = !filters.status || 
        (item.status && item.status.toLowerCase().includes(filters.status.toLowerCase())) ||
        (item.state && item.state.toLowerCase().includes(filters.status.toLowerCase()));
      
      return matchesSite && matchesType && matchesStatus;
    });
  };

  // Group storage data by site
  const getStorageBySite = (storageData) => {
    if (!storageData) return {};
    
    const grouped = {};
    storageData.forEach(storage => {
      const siteName = storage.parent_site_name || storage.parent_node_name || 'Unknown Site';
      if (!grouped[siteName]) {
        grouped[siteName] = [];
      }
      grouped[siteName].push(storage);
    });
    
    return grouped;
  };

  // Enable/Disable selected services
  const updateServiceStatus = async (action) => {
    if (selectedServiceIds.length === 0) return;
    setStatus({ type: 'info', message: `${action === 'enable' ? 'Enabling' : 'Disabling'} selected services...` });
    let successCount = 0;
    let failCount = 0;
    for (const id of selectedServiceIds) {
      try {
        await axios.post(`${API_BASE}/auth/site/service/${id}/toggle`, { enable: action === 'enable' });
        successCount++;
      } catch (e) {
        failCount++;
      }
    }
    setStatus({
      type: failCount === 0 ? 'success' : 'warning',
      message: `${action === 'enable' ? 'Enabled' : 'Disabled'} ${successCount} service(s).${failCount ? ' Failed: ' + failCount : ''}`
    });
    setSelectedServiceIds([]);
    // Refresh services
    if (filters.site) {
      setLoadingServiceData(true);
      axios.get(`${API_BASE}/auth/site/services?node_name=${encodeURIComponent(filters.site)}`)
        .then(response => {
          setServiceData({ services: response.data.data });
        })
        .finally(() => setLoadingServiceData(false));
    }
  };

  // Load initial data
  useEffect(() => {
    const fetchSites = async () => {
      try {
        const response = await axios.get(`${API_BASE}/auth/site/sites`);
        setSitesList(response.data.data || []);
      } catch (error) {
        console.error('Failed to load sites:', error);
        if (error.response?.status === 503) {
          setStatus({ 
            type: 'error', 
            message: 'Authentication server is currently unavailable. Please try again later.' 
          });
        } else if (error.response?.status === 401) {
          // Authentication required - this is expected when no tokens are available
          setStatus({ type: 'info', message: 'Authentication required. Please request a token to access site data.' });
        }
      }
    };

    loadTokens();
    fetchSites();
    checkApiStatus(); // Check API status on initial load
  }, []);

  // Auto-refresh API status every 30 seconds
  useEffect(() => {
    const interval = setInterval(() => {
      checkApiStatus();
    }, 30000); // 30 seconds

    return () => clearInterval(interval);
  }, []);



  // When a site is selected, load services and storage for that site
  useEffect(() => {
    if (filters.site) {
      setSelectedService('site-capabilities-api');
      setLoadingServiceData(true);
      setServiceData(null);
      setStorageData(null); // Clear storage data when site changes
      
      // Load services for the selected site
      axios.get(`${API_BASE}/auth/site/services?node_name=${encodeURIComponent(filters.site)}`)
        .then(response => {
          setServiceData({ services: response.data.data });
          setStatus({ type: 'success', message: `Loaded services for site ${filters.site}` });
        })
        .catch(error => {
          if (error.response?.status === 503) {
            setStatus({ 
              type: 'error', 
              message: 'Authentication server is currently unavailable. Please try again later.' 
            });
          } else {
          setStatus({ type: 'error', message: `Failed to load services for site ${filters.site}: ${error.message}` });
          }
        })
        .finally(() => setLoadingServiceData(false));
      
      // Load storage data for the selected site
      loadStorageData(filters.site);
    } else {
      setServiceData(null);
      setSelectedService(null);
      setStorageData(null);
    }
  }, [filters.site]);

  // Mapping from aud to friendly service name
  const serviceNameMap = {
    'authn-api': 'Authentication',
    'site-capabilities-api': 'Site Capabilities',
    'data-management-api': 'Data Management',
    // Add more mappings as needed
  };

  return (
    <>
      <div className="header" style={{ justifyContent: 'center' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <img src={skaLogo} alt="SKAO Logo" className="ska-logo" style={{ height: '90px', marginRight: '1.5rem' }} />
          <h1 style={{ fontSize: '4.5rem', margin: 0, lineHeight: 1, letterSpacing: '0.02em', display: 'flex', alignItems: 'center' }}>
            Operator Client
          </h1>
        </div>
      </div>

      <div className="container">
        {/* Status Messages */}
        {status && (
          <div className={`status ${status.type}`}>
            {status.message}
          </div>
        )}

        {/* API Status Panel */}
        <div className="card">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
            <div>
              <h2>System Status</h2>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginTop: '0.25rem' }}>
                <p style={{ fontSize: '0.8rem', color: '#6c757d', margin: 0 }}>
                  Auto-refreshes every 30 seconds
                </p>
                {(() => {
                  console.log('Current backend lastCheck:', apiStatus.backend.lastCheck);
                  return apiStatus.backend.lastCheck ? (
                    <span style={{ fontSize: '0.8rem', color: '#6c757d' }}>
                      • Last checked: {apiStatus.backend.lastCheck.toLocaleTimeString()}
                    </span>
                  ) : (
                    <span style={{ fontSize: '0.8rem', color: '#6c757d' }}>
                      • Checking status...
                    </span>
                  );
                })()}
                {areAllSystemsGreen() && (
                  <span style={{ 
                    fontSize: '0.8rem', 
                    color: '#28a745', 
                    fontWeight: 'bold',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.25rem'
                  }}>
                    <span style={{ 
                      width: '8px', 
                      height: '8px', 
                      backgroundColor: '#28a745', 
                      borderRadius: '50%',
                      display: 'inline-block'
                    }}></span>
                    All Systems Online
                  </span>
                )}
                {!areAllSystemsGreen() && (
                  <div style={{ 
                    display: 'flex', 
                    flexDirection: 'column', 
                    gap: '0.25rem',
                    alignItems: 'flex-start'
                  }}>
                    {!areCoreSystemsOnline() && (
                      <span style={{ 
                        fontSize: '0.8rem', 
                        color: '#dc3545', 
                        fontWeight: 'bold',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '0.25rem'
                      }}>
                        <span style={{ 
                          width: '8px', 
                          height: '8px', 
                          backgroundColor: '#dc3545', 
                          borderRadius: '50%',
                          display: 'inline-block'
                        }}></span>
                        Token Exchange Disabled - Core Systems Offline
                      </span>
                    )}
                    {areCoreSystemsOnline() && !isSiteCapabilitiesExchangeAvailable() && (
                      <span style={{ 
                        fontSize: '0.8rem', 
                        color: '#ffc107', 
                        fontWeight: 'bold',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '0.25rem'
                      }}>
                        <span style={{ 
                          width: '8px', 
                          height: '8px', 
                          backgroundColor: '#ffc107', 
                          borderRadius: '50%',
                          display: 'inline-block'
                        }}></span>
                        Site Capabilities Exchange Disabled
                      </span>
                    )}
                    {areCoreSystemsOnline() && isSiteCapabilitiesExchangeAvailable() && !isDataManagementExchangeAvailable() && (
                      <span style={{ 
                        fontSize: '0.8rem', 
                        color: '#ffc107', 
                        fontWeight: 'bold',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '0.25rem'
                      }}>
                        <span style={{ 
                          width: '8px', 
                          height: '8px', 
                          backgroundColor: '#ffc107', 
                          borderRadius: '50%',
                          display: 'inline-block'
                        }}></span>
                        Data Management Exchange Disabled
                      </span>
                    )}
                    {areCoreSystemsOnline() && isSiteCapabilitiesExchangeAvailable() && isDataManagementExchangeAvailable() && !areAllSystemsGreen() && (
                      <span style={{ 
                        fontSize: '0.8rem', 
                        color: '#6c757d', 
                        fontWeight: 'bold',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '0.25rem'
                      }}>
                        <span style={{ 
                          width: '8px', 
                          height: '8px', 
                          backgroundColor: '#6c757d', 
                          borderRadius: '50%',
                          display: 'inline-block'
                        }}></span>
                        Other Systems Offline
                      </span>
                    )}
                  </div>
                )}
              </div>
            </div>
            <button 
              className="button secondary small"
              onClick={checkApiStatus}
              disabled={checkingApiStatus}
            >
              {checkingApiStatus ? 'Checking...' : 'Refresh Status'}
            </button>
          </div>
          
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '1rem' }}>
            {/* Backend Status */}
            <div className={`status-box ${apiStatus.backend.status}`}>
              <h3>Backend Server</h3>
              <div className="status-indicator">
                <span className={`status-dot ${apiStatus.backend.status}`}></span>
                <span className="status-text">
                  {apiStatus.backend.status === 'online' ? 'Online' : 
                   apiStatus.backend.status === 'offline' ? 'Offline' : 'Unknown'}
                </span>
              </div>
              {apiStatus.backend.error && (
                <p className="error-message">{apiStatus.backend.error}</p>
              )}
            </div>

            {/* Auth Status */}
            <div className={`status-box ${apiStatus.auth.status}`}>
              <h3>Authentication</h3>
              <div className="status-indicator">
                <span className={`status-dot ${apiStatus.auth.status}`}></span>
                <span className="status-text">
                  {apiStatus.auth.status === 'online' ? 'Online' : 
                   apiStatus.auth.status === 'offline' ? 'Offline' : 'Unknown'}
                </span>
              </div>
              {apiStatus.auth.error && (
                <p className="error-message">{apiStatus.auth.error}</p>
              )}
            </div>

            {/* Permissions Status */}
            <div className={`status-box ${apiStatus.permissions.status}`}>
              <h3>Permissions</h3>
              <div className="status-indicator">
                <span className={`status-dot ${apiStatus.permissions.status}`}></span>
                <span className="status-text">
                  {apiStatus.permissions.status === 'online' ? 'Online' : 
                   apiStatus.permissions.status === 'offline' ? 'Offline' : 'Unknown'}
                </span>
              </div>
              {apiStatus.permissions.error && (
                <p className="error-message">{apiStatus.permissions.error}</p>
              )}
            </div>

            {/* Site Capabilities Status */}
            <div className={`status-box ${apiStatus['site-capabilities'].status}`}>
              <h3>Site Capabilities</h3>
              <div className="status-indicator">
                <span className={`status-dot ${apiStatus['site-capabilities'].status}`}></span>
                <span className="status-text">
                  {apiStatus['site-capabilities'].status === 'online' ? 'Online' : 
                   apiStatus['site-capabilities'].status === 'offline' ? 'Offline' : 'Unknown'}
                </span>
              </div>
              {apiStatus['site-capabilities'].error && (
                <p className="error-message">{apiStatus['site-capabilities'].error}</p>
              )}
            </div>

            {/* Data Management Status */}
            <div className={`status-box ${apiStatus['data-management'].status}`}>
              <h3>Data Management</h3>
              <div className="status-indicator">
                <span className={`status-dot ${apiStatus['data-management'].status}`}></span>
                <span className="status-text">
                  {apiStatus['data-management'].status === 'online' ? 'Online' : 
                   apiStatus['data-management'].status === 'offline' ? 'Offline' : 'Unknown'}
                </span>
              </div>
              {apiStatus['data-management'].error && (
                <p className="error-message">{apiStatus['data-management'].error}</p>
              )}
            </div>

            {/* IAM Status */}
            <div className={`status-box ${apiStatus.iam.status}`}>
              <h3>IAM</h3>
              <div className="status-indicator">
                <span className={`status-dot ${apiStatus.iam.status}`}></span>
                <span className="status-text">
                  {apiStatus.iam.status === 'online' ? 'Online' : 
                   apiStatus.iam.status === 'offline' ? 'Offline' : 'Unknown'}
                </span>
              </div>
              {apiStatus.iam.error && (
                <p className="error-message">{apiStatus.iam.error}</p>
              )}
            </div>

            {/* FTS Status */}
            <div className={`status-box ${apiStatus.fts.status}`}>
              <h3>FTS</h3>
              <div className="status-indicator">
                <span className={`status-dot ${apiStatus.fts.status}`}></span>
                <span className="status-text">
                  {apiStatus.fts.status === 'online' ? 'Online' : 
                   apiStatus.fts.status === 'offline' ? 'Offline' : 'Unknown'}
                </span>
              </div>
              {apiStatus.fts.error && (
                <p className="error-message">{apiStatus.fts.error}</p>
              )}
            </div>

            {/* Rucio Status */}
            <div className={`status-box ${apiStatus.rucio.status}`}>
              <h3>Rucio</h3>
              <div className="status-indicator">
                <span className={`status-dot ${apiStatus.rucio.status}`}></span>
                <span className="status-text">
                  {apiStatus.rucio.status === 'online' ? 'Online' : 
                   apiStatus.rucio.status === 'offline' ? 'Offline' : 'Unknown'}
                </span>
              </div>
              {apiStatus.rucio.error && (
                <p className="error-message">{apiStatus.rucio.error}</p>
              )}
            </div>

            {/* Gateway Status */}
            <div className={`status-box ${apiStatus.gateway.status}`}>
              <h3>Gateway</h3>
              <div className="status-indicator">
                <span className={`status-dot ${apiStatus.gateway.status}`}></span>
                <span className="status-text">
                  {apiStatus.gateway.status === 'online' ? 'Online' : 
                   apiStatus.gateway.status === 'offline' ? 'Offline' : 'Unknown'}
                </span>
              </div>
              {apiStatus.gateway.error && (
                <p className="error-message">{apiStatus.gateway.error}</p>
              )}
            </div>

            {/* Gatekeeper Status */}
            <div className={`status-box ${apiStatus.gatekeeper.status}`}>
              <h3>Gatekeeper</h3>
              <div className="status-indicator">
                <span className={`status-dot ${apiStatus.gatekeeper.status}`}></span>
                <span className="status-text">
                  {apiStatus.gatekeeper.status === 'online' ? 'Online' : 
                   apiStatus.gatekeeper.status === 'offline' ? 'Offline' : 'Unknown'}
                </span>
              </div>
              {apiStatus.gatekeeper.error && (
                <p className="error-message">{apiStatus.gatekeeper.error}</p>
              )}
            </div>

            {/* CANFAR Status */}
            <div className={`status-box ${apiStatus.canfar.status}`}>
              <h3>CANFAR</h3>
              <div className="status-indicator">
                <span className={`status-dot ${apiStatus.canfar.status}`}></span>
                <span className="status-text">
                  {apiStatus.canfar.status === 'online' ? 'Online' : 
                   apiStatus.canfar.status === 'offline' ? 'Offline' : 'Unknown'}
                </span>
              </div>
              {apiStatus.canfar.error && (
                <p className="error-message">{apiStatus.canfar.error}</p>
              )}
            </div>

            {/* SODA Status */}
            <div className={`status-box ${apiStatus.soda.status}`}>
              <h3>SODA</h3>
              <div className="status-indicator">
                <span className={`status-dot ${apiStatus.soda.status}`}></span>
                <span className="status-text">
                  {apiStatus.soda.status === 'online' ? 'Online' : 
                   apiStatus.soda.status === 'offline' ? 'Offline' : 'Unknown'}
                </span>
              </div>
              {apiStatus.soda.error && (
                <p className="error-message">{apiStatus.soda.error}</p>
              )}
            </div>

            {/* Prepare Data Status */}
            <div className={`status-box ${apiStatus['prepare-data'].status}`}>
              <h3>Prepare Data</h3>
              <div className="status-indicator">
                <span className={`status-dot ${apiStatus['prepare-data'].status}`}></span>
                <span className="status-text">
                  {apiStatus['prepare-data'].status === 'online' ? 'Online' : 
                   apiStatus['prepare-data'].status === 'offline' ? 'Offline' : 'Unknown'}
                </span>
              </div>
              {apiStatus['prepare-data'].error && (
                <p className="error-message">{apiStatus['prepare-data'].error}</p>
              )}
            </div>

          </div>
        </div>

        {/* Token Management Section - Two Column Layout */}
        <div className="two-column-layout">
          {/* Left Column - Request New Token */}
          <div className="card left-column">
            <h2>Request New Token</h2>
            <div className="device-flow-info">
              <p><strong>Step 1:</strong> Click the button below to start the device flow authentication process.</p>
              <p><strong>Step 2:</strong> You'll be redirected to the authentication server to log in.</p>
              <p><strong>Step 3:</strong> After successful authentication, you'll be redirected back with a token.</p>
              {!areCoreSystemsOnline() && (
                <div style={{ marginTop: '1rem', padding: '1rem', backgroundColor: '#fff3cd', border: '1px solid #ffeaa7', borderRadius: '4px' }}>
                  <h4 style={{ color: '#856404', margin: '0 0 0.5rem 0' }}>⚠️ Core Systems Offline</h4>
                  <p style={{ color: '#856404', margin: '0', fontSize: '0.9rem' }}>
                    Token requests are disabled because one or more core systems (Authentication, Permissions, Backend) are currently offline.
                  </p>
                </div>
              )}
              <div style={{ marginTop: '1rem' }}>
                <button 
                  className="button"
                  onClick={requestToken}
                  disabled={loading || pollingInterval || !areCoreSystemsOnline()}
                  title={!areCoreSystemsOnline() ? 'Authentication, Permissions, or Backend is offline. Cannot request tokens when core systems are unavailable.' : ''}
                >
                  {loading && <span className="loading"></span>}
                  {loading ? 'Requesting Token...' : 'Request Token'}
                </button>
              </div>
            </div>
            {tokenRequest && (
              <div className="device-flow-info">
                <h3>Device Flow Authentication</h3>
                <p>Please complete authentication using one of the following methods:</p>
                
                <div>
                  <strong>User Code:</strong>
                  <div className="code">{tokenRequest.user_code}</div>
                </div>
                
                <div>
                  <strong>Verification URI:</strong>
                  <br />
                  <a 
                    href={tokenRequest.verification_uri} 
                    target="_blank" 
                    rel="noopener noreferrer"
                    className="link"
                  >
                    {tokenRequest.verification_uri}
                  </a>
                </div>
                
                {tokenRequest.verification_uri_complete && (
                  <div>
                    <strong>Complete URI (with code):</strong>
                    <br />
                    <a 
                      href={tokenRequest.verification_uri_complete} 
                      target="_blank" 
                      rel="noopener noreferrer"
                      className="link"
                    >
                      {tokenRequest.verification_uri_complete}
                    </a>
                  </div>
                )}
                
                <p><strong>Expires in:</strong> {tokenRequest.expires_in} seconds</p>
                <p><strong>Polling interval:</strong> {tokenRequest.interval} seconds</p>
                
                {pollingInterval && (
                  <div className="status info">
                    <span className="loading"></span>
                    Polling for authentication completion...
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Right Column - Existing Tokens */}
          <div className="card right-column">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
              <div>
                <h2>Existing Tokens</h2>
                <p style={{ fontSize: '0.9rem', color: '#6c757d', margin: '0.25rem 0 0 0' }}>
                  {tokens.length} token{tokens.length !== 1 ? 's' : ''} loaded
                </p>
              </div>
              <div style={{ display: 'flex', gap: '0.5rem' }}>
                <button className="button secondary small" onClick={loadTokens}
                  disabled={!areCoreSystemsOnline()}
                  title={!areCoreSystemsOnline() ? 'Authentication, Permissions, or Backend is offline. Cannot refresh tokens when core systems are unavailable.' : ''}
                >
                  Refresh Tokens
                </button>
                <button className="button danger small" onClick={() => {
                  if (window.confirm('Are you sure you want to delete all tokens? This action cannot be undone.')) {
                    tokens.forEach(token => deleteToken(token.file_name));
                  }
                }} disabled={tokens.length === 0}>
                  Delete All
                </button>
              </div>
            </div>
            
            {tokens.length === 0 ? (
              <p>No tokens found. Request a new token to get started.</p>
            ) : (
              <div className="token-scroll-container" style={{ 
                display: 'flex',
                overflowX: 'auto',
                gap: '1rem',
                padding: '0.5rem',
                backgroundColor: '#f8f9fa',
                borderRadius: '4px',
                border: '1px solid #e0e0e0',
                minHeight: '400px',
                alignItems: 'flex-start',
                width: '100%',
                maxWidth: '100%'
              }}
              >
                {tokens.map((token, index) => (
                  <div key={token.file_name} className="device-flow-info" style={{ 
                    width: '300px',
                    minWidth: '300px',
                    minHeight: '420px',
                    padding: '1rem', 
                    border: '1px solid #dee2e6', 
                    borderRadius: '8px',
                    backgroundColor: 'white',
                    position: 'relative',
                    boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                    flexShrink: 0
                  }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '0.5rem' }}>
                      <div>
                        <h4 style={{ margin: 0 }}>{serviceNameMap[token.service_name] || token.service_name || 'Unknown Service'}</h4>
                        <div style={{ fontSize: '0.8rem', color: '#888' }}>File: {token.file_name}</div>
                        {activeTokens[token.service_name] === token.file_name && (
                          <span style={{ color: '#28a745', fontWeight: 'bold', fontSize: '0.85rem' }}>Active</span>
                        )}
                      </div>
                    </div>
                    <p><strong>Access Token:</strong> {token.access_token || 'Not available'}</p>
                    <p><strong>Expires (Local):</strong> {token.expires_local || 'Unknown'}</p>
                    <p><strong>Expires (UTC):</strong> {token.expires_utc || 'Unknown'}</p>
                    <p><strong>Has Refresh Token:</strong> {token.has_refresh_token ? 'Yes' : 'No'}</p>
                    <p><strong>Path:</strong> {token.path_on_disk || 'Unknown'}</p>
                    
                    <div className="token-actions">
                      <h5>Exchange for other services:</h5>
                      <div className="service-buttons">
                        {availableServices.map((service) => {
                          // Determine if this specific service exchange should be disabled
                          let isDisabled = service === token.service_name;
                          let tooltip = '';
                          
                          if (!isDisabled) {
                            if (!areCoreSystemsOnline()) {
                              isDisabled = true;
                              tooltip = 'Authentication, Permissions, or Backend is offline';
                            } else if (service === 'site-capabilities-api' && !isSiteCapabilitiesExchangeAvailable()) {
                              isDisabled = true;
                              tooltip = 'Site Capabilities service is offline';
                            } else if (service === 'data-management-api' && !isDataManagementExchangeAvailable()) {
                              isDisabled = true;
                              tooltip = 'Data Management service is offline';
                            }
                          }
                          
                          return (
                            <button 
                              key={service}
                              className={`service-exchange-btn${clickedExchange === service ? ' clicked' : ''}`}
                              onClick={() => exchangeToken(service)}
                              disabled={isDisabled}
                              title={tooltip || (service === token.service_name ? 'Cannot exchange for same service' : '')}
                            >
                              {service}
                            </button>
                          );
                        })}
                      </div>
                    </div>
                    
                    <div style={{ display: 'flex', gap: '0.5rem', marginTop: '1rem', flexWrap: 'wrap' }}>
                      <button 
                        className="button secondary small"
                        onClick={() => setActiveToken(token.service_name, token.file_name)}
                        disabled={activeTokens[token.service_name] === token.file_name}
                        style={{ fontSize: '0.75rem', padding: '0.25rem 0.5rem' }}
                      >
                        Set Active
                      </button>
                      <button 
                        className="button small"
                        onClick={() => refreshToken(token.file_name)}
                        disabled={apiStatus.auth.status === 'offline'}
                        title={apiStatus.auth.status === 'offline' ? 'Authentication service is offline. Cannot refresh tokens when authentication server is unavailable.' : ''}
                        style={{ fontSize: '0.75rem', padding: '0.25rem 0.5rem' }}
                      >
                        Refresh
                      </button>
                      <button 
                        className="button danger small"
                        onClick={() => {
                          if (window.confirm(`Are you sure you want to delete the token file ${token.file_name}?`)) {
                            deleteToken(token.file_name);
                          }
                        }}
                        style={{ fontSize: '0.75rem', padding: '0.25rem 0.5rem' }}
                      >
                        Delete
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Service Functions Section - Full Width */}
        <div className="card">
            <h2>Service Functions</h2>
            
            {/* Site Selection - Always Visible */}
            <div className="filters">
              <h4>Select Site</h4>
              <div className="filter-row">
                <select 
                  value={filters.site || ''} 
                  onChange={(e) => setFilters({...filters, site: e.target.value})}
                  className="filter-select"
                >
                  <option value="">Select a site to view services...</option>
                  {sitesList.map((site, index) => (
                    <option key={index} value={site.node}>{site.name || site.node}</option>
                  ))}
                </select>
              </div>
            </div>

            {loadingServiceData ? (
              <div className="status info">Loading services for selected site...</div>
            ) : selectedService && serviceData?.services ? (
              <div>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
                  <h3>Services for {filters.site}</h3>
                  <button 
                    className="button secondary small"
                    onClick={() => {
                      setLoadingServiceData(true);
                      axios.get(`${API_BASE}/auth/site/services?node_name=${encodeURIComponent(filters.site)}`)
                        .then(response => {
                          setServiceData({ services: response.data.data });
                          setStatus({ type: 'success', message: `Refreshed services for site ${filters.site}` });
                        })
                        .catch(error => {
                          setStatus({ type: 'error', message: `Failed to refresh services: ${error.message}` });
                        })
                        .finally(() => setLoadingServiceData(false));
                    }}
                    disabled={loadingServiceData}
                  >
                    {loadingServiceData ? 'Refreshing...' : 'Refresh Status'}
                  </button>
                </div>
                
                {/* Additional Filters */}
                <div className="filters">
                  <h4>Additional Filters</h4>
                  <div className="filter-row">
                    <select 
                      value={filters.serviceType || ''} 
                      onChange={(e) => setFilters({...filters, serviceType: e.target.value})}
                      className="filter-select"
                    >
                      <option value="">All Service Types</option>
                      {[...new Set(serviceData?.services?.map(s => s.type).filter(Boolean))].map((type, index) => (
                        <option key={index} value={type}>{type}</option>
                      ))}
                    </select>
                    
                    <select 
                      value={filters.status || ''} 
                      onChange={(e) => setFilters({...filters, status: e.target.value})}
                      className="filter-select"
                    >
                      <option value="">All Statuses</option>
                      {[...new Set([
                        ...(serviceData?.services?.map(s => s.status).filter(Boolean) || []),
                        ...(serviceData?.sites?.map(s => s.status).filter(Boolean) || []),
                        ...(serviceData?.compute?.map(c => c.status).filter(Boolean) || [])
                      ])].map((status, index) => (
                        <option key={index} value={status}>{status}</option>
                      ))}
                    </select>
                  </div>
                </div>

                {/* Status Legend */}
                <div className="status-legend" style={{ marginBottom: '1rem', padding: '0.5rem', backgroundColor: '#f8f9fa', borderRadius: '0.25rem', fontSize: '0.85rem' }}>
                  <strong>Status Legend:</strong>
                  <span className="status-indicator up" style={{ marginLeft: '0.5rem' }}>up ●</span>
                  <span className="status-indicator down" style={{ marginLeft: '0.5rem' }}>down ●</span>
                  <span className="status-indicator unknown" style={{ marginLeft: '0.5rem' }}>unknown</span>
                  <span style={{ marginLeft: '0.5rem', color: '#6c757d' }}>● = Real-time status</span>
                </div>

                {/* Enable/Disable Buttons Above Service List */}
                <div style={{ marginBottom: '1rem', display: 'flex', gap: '1rem' }}>
                  <button className="button" disabled={selectedServiceIds.length === 0} onClick={() => updateServiceStatus('enable')}>Enable</button>
                  <button className="button secondary" disabled={selectedServiceIds.length === 0} onClick={() => updateServiceStatus('disable')}>Disable</button>
                </div>

                {/* Services */}
                {serviceData?.services && (
                  <div>
                    <div className="section-header"
                      onClick={() => setCollapsedSections({...collapsedSections, services: !collapsedSections.services})}
                    >
                      <h4>Services ({getFilteredData(serviceData.services, 'service').length})</h4>
                      <span className="collapse-icon">{collapsedSections.services ? '▼' : '▲'}</span>
                    </div>
                    {!collapsedSections.services && (
                      <div className="data-list">
                        {getFilteredData(serviceData.services, 'service').map((service, index) => (
                          <div key={index} className="data-item" style={{ display: 'flex', alignItems: 'center' }}>
                            <input
                              type="checkbox"
                              checked={selectedServiceIds.includes(service.id)}
                              onChange={e => {
                                setSelectedServiceIds(e.target.checked
                                  ? [...selectedServiceIds, service.id]
                                  : selectedServiceIds.filter(id => id !== service.id));
                              }}
                              style={{ marginRight: 8 }}
                            />
                            <div style={{ flex: 1 }}>
                              <strong>{service.name || service.id || 'Unknown'}</strong>
                              <table className="service-details-table">
                                <tbody>
                                  {service.type && (
                                    <tr><td>Type</td><td>{service.type}</td></tr>
                                  )}
                                  {service.site && (
                                    <tr><td>Site</td><td>{service.site}</td></tr>
                                  )}
                                  {service.site_name && !service.site && (
                                    <tr><td>Site</td><td>{service.site_name}</td></tr>
                                  )}
                                  {service.node && (
                                    <tr><td>Node</td><td>{service.node}</td></tr>
                                  )}
                                  {service.host && (
                                    <tr><td>Host</td><td>{service.host}</td></tr>
                                  )}
                                  {service.port && (
                                    <tr><td>Port</td><td>{service.port}</td></tr>
                                  )}
                                  {service.path && (
                                    <tr><td>Path</td><td>{service.path}</td></tr>
                                  )}
                                  {service.prefix && (
                                    <tr><td>Prefix</td><td>{service.prefix}</td></tr>
                                  )}
                                  {service.assoc_storage_id && (
                                    <tr><td>Assoc Storage ID</td><td>{service.assoc_storage_id}</td></tr>
                                  )}
                                  {service.parent_compute_id && (
                                    <tr><td>Parent Compute ID</td><td>{service.parent_compute_id}</td></tr>
                                  )}
                                  {service.scope && (
                                    <tr><td>Scope</td><td>{service.scope}</td></tr>
                                  )}
                                  {service.status && (
                                    <tr>
                                      <td>Status</td>
                                      <td>
                                        <span className={`status-indicator ${service.status}`}>
                                          {service.status}
                                          {service.real_time_status && (
                                            <span className="real-time-badge" title="Real-time status">●</span>
                                          )}
                                        </span>
                                      </td>
                                    </tr>
                                  )}
                                  {service.state && !service.status && (
                                    <tr><td>State</td><td>{service.state}</td></tr>
                                  )}
                                  {service.id && (
                                    <tr><td>ID</td><td>{service.id}</td></tr>
                                  )}
                                </tbody>
                              </table>
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}
                
                {/* Storage Resources */}
                {filters.site && (
                  <div style={{ marginTop: '2rem' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
                      <h3>Storage Resources for {filters.site}</h3>
                      <button 
                        className="button secondary small"
                        onClick={() => loadStorageData(filters.site)}
                        disabled={loadingStorageData}
                      >
                        {loadingStorageData ? 'Loading...' : 'Refresh Storage'}
                      </button>
                    </div>
                    
                    {loadingStorageData ? (
                      <div className="status info">Loading storage resources for {filters.site}...</div>
                    ) : storageData ? (
                      <div>
                        <div className="section-header"
                          onClick={() => setCollapsedSections({...collapsedSections, storage: !collapsedSections.storage})}
                        >
                          <h4>Storage Resources ({storageData.length})</h4>
                          <span className="collapse-icon">{collapsedSections.storage ? '▼' : '▲'}</span>
                        </div>
                        {!collapsedSections.storage && (
                          <div>
                            {Object.entries(getStorageBySite(storageData)).map(([siteName, storages]) => (
                              <div key={siteName} className="site-storage-group">
                                <h5 style={{ marginBottom: '0.5rem', color: '#E70068' }}>{siteName} ({storages.length})</h5>
                                <div className="data-list">
                                  {storages.map((storage, index) => (
                                    <div key={index} className="data-item">
                                      <strong>{storage.name || storage.id || 'Unknown Storage'}</strong>
                                      <table className="service-details-table">
                                        <tbody>
                                          {storage.host && (
                                            <tr><td>Host</td><td>{storage.host}</td></tr>
                                          )}
                                          {storage.base_path && (
                                            <tr><td>Base Path</td><td>{storage.base_path}</td></tr>
                                          )}
                                          {storage.srm && (
                                            <tr><td>SRM</td><td>{storage.srm}</td></tr>
                                          )}
                                          {storage.device_type && (
                                            <tr><td>Device Type</td><td>{storage.device_type}</td></tr>
                                          )}
                                          {storage.size_in_terabytes && (
                                            <tr><td>Size</td><td>{storage.size_in_terabytes} TB</td></tr>
                                          )}
                                          {storage.supported_protocols && storage.supported_protocols.length > 0 && (
                                            <tr>
                                              <td>Protocols</td>
                                              <td>
                                                {storage.supported_protocols.map((protocol, pIndex) => (
                                                  <span key={pIndex}>
                                                    {protocol.prefix}://{storage.host}:{protocol.port}
                                                    {pIndex < storage.supported_protocols.length - 1 ? ', ' : ''}
                                                  </span>
                                                ))}
                                              </td>
                                            </tr>
                                          )}
                                          {storage.areas && storage.areas.length > 0 && (
                                            <tr>
                                              <td>Areas</td>
                                              <td>
                                                {storage.areas.map((area, aIndex) => (
                                                  <div key={aIndex} style={{ marginBottom: '0.5rem' }}>
                                                    <strong>{area.name}</strong> ({area.type})
                                                    {area.tier !== undefined && <span> - Tier {area.tier}</span>}
                                                    {area.relative_path && <div style={{ fontSize: '0.9em', color: '#666' }}>Path: {area.relative_path}</div>}
                                                  </div>
                                                ))}
                                              </td>
                                            </tr>
                                          )}
                                          {storage.downtime && storage.downtime.length > 0 && (
                                            <tr>
                                              <td>Downtime</td>
                                              <td style={{ color: '#dc3545' }}>
                                                {storage.downtime.map((dt, dtIndex) => (
                                                  <div key={dtIndex}>
                                                    {dt.date_range} - {dt.type}: {dt.reason}
                                                  </div>
                                                ))}
                                              </td>
                                            </tr>
                                          )}
                                          {storage.is_force_disabled && (
                                            <tr>
                                              <td>Status</td>
                                              <td style={{ color: '#dc3545' }}>Force Disabled</td>
                                            </tr>
                                          )}
                                          {storage.id && (
                                            <tr><td>ID</td><td>{storage.id}</td></tr>
                                          )}
                                        </tbody>
                                      </table>
                                    </div>
                                  ))}
                                </div>
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    ) : (
                      <p>No storage data available for {filters.site}.</p>
                    )}
                  </div>
                )}
                
                {/* Compute Resources */}
                {serviceData?.compute && (
                  <div>
                    <div 
                      className="section-header"
                      onClick={() => setCollapsedSections({...collapsedSections, compute: !collapsedSections.compute})}
                    >
                      <h4>Compute Resources ({getFilteredData(serviceData.compute, 'compute').length})</h4>
                      <span className="collapse-icon">{collapsedSections.compute ? '▼' : '▲'}</span>
                    </div>
                    {!collapsedSections.compute && (
                      <div className="data-list">
                        {getFilteredData(serviceData.compute, 'compute').map((comp, index) => (
                          <div key={index} className="data-item">
                            <strong>{comp.name || comp.id || 'Unknown'}</strong>
                            <p>Type: {comp.type || 'Unknown'}</p>
                            <p>Status: {comp.status || comp.state || 'Unknown'}</p>
                            {comp.site && <p>Site: {comp.site}</p>}
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </div>
            ) : (
              <p>Select a site from the dropdown above to see available services.</p>
            )}
          </div>
        </div>
    </>
  );
}

export default App; 