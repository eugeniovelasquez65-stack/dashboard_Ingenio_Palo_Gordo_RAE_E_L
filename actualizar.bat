@echo off
chcp 65001 >nul
set "RUTA=C:\Users\eulopez\Documents\EUGENIO_LÓPEZ\CONTROLES IPG_REA.E.L\DASHBOARD_WEB"
git -C "%RUTA%" add .
git -C "%RUTA%" commit -m "actualizo datos"
git -C "%RUTA%" push
echo Datos actualizados correctamente!
pause