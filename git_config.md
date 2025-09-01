# Configuración de GitHub para este proyecto

## Pasos para configurar tu token de GitHub:

### 1. Generar un Personal Access Token
1. Ve a GitHub.com → Settings → Developer settings → Personal access tokens → Tokens (classic)
2. Click en "Generate new token (classic)"
3. Dale un nombre descriptivo (ej: "MIO_git project")
4. Selecciona los scopes necesarios (mínimo: `repo`, `workflow`)
5. Copia el token generado

### 2. Configurar Git localmente
```powershell
# Configurar usuario y email
git config --global user.name "Tu Nombre de Usuario"
git config --global user.email "tu.email@ejemplo.com"

# Configurar credenciales
git config --global credential.helper store
```

### 3. Usar el token
Cuando hagas push/pull, Git te pedirá credenciales:
- Username: tu_usuario_de_github
- Password: tu_token_aqui (NO tu contraseña de GitHub)

### 4. Alternativa: Configurar remoto con token
```powershell
git remote set-url origin https://tu_usuario:tu_token_aqui@github.com/usuario/MIO_git.git
```

## Notas importantes:
- NUNCA subas tu token al repositorio
- El archivo .env está en .gitignore por seguridad
- Si usas el token en la URL, asegúrate de que no se guarde en el historial
- Considera usar SSH keys como alternativa más segura
