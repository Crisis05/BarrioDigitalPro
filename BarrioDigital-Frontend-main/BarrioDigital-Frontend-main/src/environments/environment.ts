export const environment = {
  production: false,
  apiUrl: '/api',
  azure: {
    clientId: 'e514a85c-e45b-426d-9427-5fd5fef5d24d',
    tenantId: '48733629-7ef9-430e-9f2c-4b3f5dbede94',
    authority: 'https://login.microsoftonline.com/48733629-7ef9-430e-9f2c-4b3f5dbede94',
    redirectUri: typeof window !== 'undefined' ? window.location.origin : 'http://localhost:4200',
    postLogoutRedirectUri: typeof window !== 'undefined' ? window.location.origin + '/login' : 'http://localhost:4200/login',
    apiEndpoint: 'https://vtk53qn89d.execute-api.us-east-1.amazonaws.com/api',
    scopes: [
      'openid',
      'profile',
      'email'
    ]
  }
};
