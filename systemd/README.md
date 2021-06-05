copy the daemon into /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl start iotcore.service
sudo systemctl status iotcore.service
sudo systemctl enable iotcore.service
sudo systemctl disable iotcore.service

