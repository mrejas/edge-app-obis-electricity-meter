--
-- edge-app-obis-electric-meter
--

-- local lynx = require("edge.lynx")
local edge = require("edge")
device_id = 0
dirty = true -- The device needs update

function hex_dump(buf)
	local ret = "";
	for byte=1, #buf, 16 do
		local chunk = buf:sub(byte, byte+15)
		chunk:gsub('.', function (c) ret = ret .. string.format('%02X',string.byte(c)) end)
	end
	return ret
end

function findFunctionMeta(meta)
        local match = 1
        for i, fun in ipairs(functions) do
                match = 1;
                for k, v in pairs(meta) do
                        if fun.meta[k] ~= v then
                                match = 0
                        end
                end
                if match == 1 then
                        return functions[i]
                end
        end
        return nil;
end

function findDeviceMeta(meta)
        devices, err = lynx.apiCall("GET", "/api/v2/devicex/" .. app.installation_id)
        local match = 1
        for i, dev in ipairs(devices) do
                match = 1;
                for k, v in pairs(meta) do
                        if dev.meta[k] ~= v then
                                match = 0
                        end
                end
                if match == 1 then
                        return devices[i]
                end
        end
        return nil;
end

function create_function_if_needed(metric, device)
	local func = findFunctionMeta({
		electric_meter_id = tostring(cfg.device),
		electric_meter_metric = metric
	})

	if func == nil then
		local fn

		if metric == "act_pow_pos_total" or metric == "act_pow_neg_total"  then
			fn = {
				type = "active_energy_total",
				installation_id = app.installation_id,
				meta = {
					name = "Electric Meter - " .. metric,
					device_id = tostring(device),
					electric_meter_id = tostring(cfg.device),
					electric_meter_metric = metric,
					unit = "Wh",
					format = "%0.0f Wh",
					topic_read = "obj/electric_meter/" .. cfg.device .. "/" .. metric
				}
			}

		elseif metric == "current_L1" or metric == "current_L2" or metric == "current_L3" then
			fn = {
				type = "current",
				installation_id = app.installation_id,
				meta = {
					name = "Electric Meter - " .. metric,
					device_id = tostring(device),
					electric_meter_id = tostring(cfg.device),
					electric_meter_metric = metric,
					unit = "A",
					format = "%0.1f A",
					topic_read = "obj/electric_meter/" .. cfg.device .. "/" .. metric
				}
			}
		elseif metric == "Uptime" then
			fn = {
				type = "uptime",
				installation_id = app.installation_id,
				meta = {
					name = "Electric Meter - " .. metric,
					device_id = tostring(device),
					electric_meter_id = tostring(cfg.device),
					electric_meter_metric = metric,
					unit = "s",
					format = "%0.0f s",
					topic_read = "obj/electric_meter/" .. cfg.device .. "/" .. metric
				}
			}
		elseif metric == "rssi" then
			fn = {
				type = "rssi",
				installation_id = app.installation_id,
				meta = {
					name = "Electric Meter - " .. metric,
					device_id = tostring(device),
					electric_meter_id = tostring(cfg.device),
					electric_meter_metric = metric,
					unit = "dB",
					format = "%0.0f dB",
					topic_read = "obj/electric_meter/" .. cfg.device .. "/" .. metric
				}
			}
		elseif metric == "voltage_L1" or metric == "voltage_L2" or metric == "voltage_L3" or 
			metric == "usbV" or metric == "Vin" then
			fn = {
				type = "voltage",
				installation_id = app.installation_id,
				meta = {
					name = "Electric Meter - " .. metric,
					device_id = tostring(device),
					electric_meter_id = tostring(cfg.device),
					electric_meter_metric = metric,
					unit = "V",
					format = "%0.1f V",
					topic_read = "obj/electric_meter/" .. cfg.device .. "/" .. metric
				}
			}
		elseif metric == "act_pow_pos" or metric == "act_pow_neg" or 
		       metric == "act_pow_pos_L1" or metric == "act_pow_neg_L1" or
		       metric == "act_pow_pos_L2" or metric == "act_pow_neg_L2" or
		       metric == "act_pow_pos_L3" or metric == "act_pow_neg_L3" then
			fn = {
				type = "active power",
				installation_id = app.installation_id,
				meta = {
					name = "Electric Meter - " .. metric,
					device_id = tostring(device),
					electric_meter_id = tostring(cfg.device),
					electric_meter_metric = metric,
					unit = "W",
					format = "%0.0f W",
					topic_read = "obj/electric_meter/" .. cfg.device .. "/" .. metric
				}
			}
		elseif metric == "react_pow_pos" or metric == "react_pow_neg" then
			fn = {
				type = "reactive power",
				installation_id = app.installation_id,
				meta = {
					name = "Electric Meter - " .. metric,
					device_id = tostring(device),
					electric_meter_id = tostring(cfg.device),
					electric_meter_metric = metric,
					unit = "VAr",
					format = "%0.0f VAr",
					topic_read = "obj/electric_meter/" .. cfg.device .. "/" .. metric
				}
			}

		elseif metric == "act_energy_pos" or metric == "act_energy_neg" then
			fn = {
				type = "active energy",
				installation_id = app.installation_id,
				meta = {
					name = "Electric Meter - " .. metric,
					device_id = tostring(device),
					electric_meter_id = tostring(cfg.device),
					electric_meter_metric = metric,
					unit = "Wh",
					format = "%0.0f Wh",
					topic_read = "obj/electric_meter/" .. cfg.device .. "/" .. metric
				}
			}
		elseif metric == "react_energy_pos" or metric == "react_energy_neg" then
			fn = {
				type = "reactive power",
				installation_id = app.installation_id,
				meta = {
					name = "Electric Meter - " .. metric,
					device_id = tostring(device),
					electric_meter_id = tostring(cfg.device),
					electric_meter_metric = metric,
					unit = "VArh",
					format = "%0.0f VArh",
					topic_read = "obj/electric_meter/" .. cfg.device .. "/" .. metric
				}
			}

		end

		lynx.createFunction(fn)
	end
end

function publish_data(metric, value, timestamp)
	create_function_if_needed(metric, device_id)

	local topic_read = "obj/electric_meter/" .. cfg.device .. "/" .. metric
	local data = json:encode({ timestamp = timestamp, value = value })
	mq:pub(topic_read, data);

end

function handleTrigger(topic, payload, retained)

	-- Every now and then a json-package is sent with the following.
	-- If it starts with { let's assume it is this message
	-- {
	--  "status": {
	--    "rssi": -79,
	--    "ch": 1,
	--    "ssid": "RejasDatakonsult",
	--    "usbV": "0.00",
	--    "Vin": "23.84",
	--    "Vcap": "3.65",
	--    "Vbck": "4.60",
	--    "Build": "1.1.15",
	--    "Hw": "F",
	--    "bssid": "6ccdd6a89e80",
	--    "ID": "e831cd4e3f3c",
	--    "Uptime": 133,
	--    "mqttcon": 1,
	--    "pubcnt": 0,
	--    "rxcnt": 0,
	--    "wificon": 3,
	--    "wififail": 2,
	--    "bits": 340,
	--    "cSet": 87,
	--    "Ic": 0,
	--    "crcerr": 0,
	--    "cAx": 1.282607,
	--    "cB": 15,
	--    "heap": 209552,
	--    "baud": 2400,
	--    "meter": "Aidon_V2",
	--    "ntc": -5.41,
	--    "s/w": 0,
	--    "ct": 0,
	--    "dtims": 38
	--  }
	-- }

	if payload:find("{", 1, true) == 1 then
		local obj = json:decode(payload)
		publish_data('rssi', obj.status.rssi, edge:time())
		publish_data('usbV', obj.status.usbV, edge:time())
		publish_data('Vin', obj.status.Vin, edge:time())
		publish_data('Uptime', obj.status.Uptime, edge:time())
		
		-- Save some data as metadata on the device
		update_device(obj)
		return -- No need to parse any further
	end


	p = string.find(payload, "0-0:1.0.0")
	if p then
		local y = tonumber("20" .. string.sub(payload, p+8, p+8+1))
		local m = tonumber(string.sub(payload, p+10, p+10+1))
		local d = tonumber(string.sub(payload, p+12, p+12+1))
		local ho = tonumber(string.sub(payload, p+14, p+14+1))
		local mi = tonumber(string.sub(payload, p+16, p+16+1))
		local se = tonumber(string.sub(payload, p+18, p+18+1))

		local timestamp = os.time{year=y, month=m, day=d, hour=ho, min=mi, sec=se}
	else
		local timestamp = os.time()
	end
	
	-- The above doesn't work for some reason, so....
	local timestamp = os.time()

	-- act_pow_pos_total
	p = string.find(payload, "1-0:1.8.0") -- E.g. 1-0:1.8.0(00005524.266*kWh)
	if p then
		local act_pow_pos_total = tonumber(string.sub(payload, p+8, p+8+11))*1000
		publish_data('act_pow_pos_total', act_pow_pos_total, timestamp)
	end
	
	-- act_pow_neg_total
	p = string.find(payload, "1-0:2.8.0")
	if p then
		local act_pow_neg_total = tonumber(string.sub(payload, p+8, p+8+11))*1000
		publish_data('act_pow_neg_total', act_pow_neg_total, timestamp)
	end

	-- current_L1 
	p = string.find(payload, "1-0:31.7.0")
	if p then
		local current_L1 = tonumber(string.sub(payload, p+9, p+9+4))
		publish_data('current_L1', current_L1, timestamp)
	end

	-- current_L2 
	p = string.find(payload, "1-0:51.7.0")
	if p then
		local current_L2 = tonumber(string.sub(payload, p+9, p+9+4))
		publish_data('current_L2', current_L2, timestamp)
	end
	
	-- current_L3 
	p = string.find(payload, "1-0:71.7.0")
	if p then
		local current_L3 = tonumber(string.sub(payload, p+9, p+9+4))
		publish_data('current_L3', current_L3, timestamp)
	end


	p = string.find(payload, "1-0:32.7.0")
	if p then
		local volt_L1 = tonumber(string.sub(payload, p+9, p+9+4))
		publish_data('voltage_L1', volt_L1, timestamp)
	end

	p = string.find(payload, "1-0:52.7.0")
	if p then
		local volt_L2 = tonumber(string.sub(payload, p+9, p+9+4))
		publish_data('voltage_L2', volt_L2, timestamp)
	end

	p = string.find(payload, "1-0:72.7.0")
	if p then
		local volt_L3 = tonumber(string.sub(payload, p+9, p+9+4))
		publish_data('voltage_L3', volt_L3, timestamp)
	end

	p = string.find(payload, "1-0:1.7.0")
	e = string.find(payload, "*", p)
	if p then
		local act_energy_pos = tonumber(string.sub(payload, p+8, e-1)*1000)
		publish_data('act_energy_pos', act_energy_pos, timestamp)
	end

	p = string.find(payload, "1-0:2.7.0")
	e = string.find(payload, "*", p)
	if p then
		local act_energy_neg = tonumber(string.sub(payload, p+8, e-1)*1000)
		publish_data('act_energy_neg', act_energy_neg, timestamp)
	end


	p = string.find(payload, "1-0:3.8.0")
	e = string.find(payload, "*", p)
	if p then
		local react_energy_pos = tonumber(string.sub(payload, p+8, e-1)*1000)
		publish_data('act_energy_pos', act_energy_pos, timestamp)
	end

	p = string.find(payload, "1-0:4.8.0")
	e = string.find(payload, "*", p)
	if p then
		local react_energy_neg = tonumber(string.sub(payload, p+8, e-1)*1000)
		publish_data('act_energy_neg', act_energy_neg, timestamp)
	end

	p = string.find(payload, "1-0:21.7.0")
	e = string.find(payload, "*", p)
	if p then
		local act_pow_pos_L1 = tonumber(string.sub(payload, p+9, e-1)*1000)
		publish_data('act_pow_pos_L1', act_pow_pos_L1, timestamp)
	end

	p = string.find(payload, "1-0:41.7.0")
	e = string.find(payload, "*", p)
	if p then
		local act_pow_pos_L2 = tonumber(string.sub(payload, p+9, e-1)*1000)
		publish_data('act_pow_pos_L2', act_pow_pos_L2, timestamp)
	end

	p = string.find(payload, "1-0:61.7.0")
	e = string.find(payload, "*", p)
	if p then
		local act_pow_pos_L3 = tonumber(string.sub(payload, p+9, e-1)*1000)
		publish_data('act_pow_pos_L3', act_pow_pos_L3, timestamp)
	end

	p = string.find(payload, "1-0:22.7.0")
	e = string.find(payload, "*", p)
	if p then
		local act_pow_neg_L1 = tonumber(string.sub(payload, p+9, e-1)*1000)
		publish_data('act_pow_neg_L1', act_pow_neg_L1, timestamp)
	end

	p = string.find(payload, "1-0:42.7.0")
	e = string.find(payload, "*", p)
	if p then
		local act_pow_neg_L2 = tonumber(string.sub(payload, p+9, e-1)*1000)
		publish_data('act_pow_neg_L2', act_pow_neg_L2, timestamp)
	end

	p = string.find(payload, "1-0:62.7.0")
	e = string.find(payload, "*", p)
	if p then
		local act_pow_neg_L3 = tonumber(string.sub(payload, p+9, e-1)*1000)
		publish_data('act_pow_neg_L3', act_pow_neg_L3, timestamp)
	end
end

function update_device(data) 
	print("Updating device")

	if dirty then

		local dev = setup_device(cfg.device)

		dev.meta.meter = data.status.meter
		dev.meta.ssid = data.status.ssid
		dev.meta.bssid = data.status.bssid
		dev.meta.ID = data.status.ID
		dev.updated = nil
		dev.created = nil
		dev.protected_meta = nil
		lynx.apiCall("PUT", "/api/v2/devicex/" .. app.installation_id .. "/" .. device_id, dev)
		dirty = false
	end
end


function setup_device(device) 
	local dev = findDeviceMeta({
		electric_meter_id = tostring(device)
	})

	if dev == nil then
		print("Creating device")
		local _dev = {
			type = "electric_meter",
			installation_id = app.installation_id,
			meta = {
				name = "Electric Meter: " .. device,
				electric_meter_id = tostring(device)
			}
		}
		
		lynx.apiCall("POST", "/api/v2/devicex/" .. app.installation_id , _dev)

		dev = findDeviceMeta({
			electric_meter_id = tostring(device)
		})
	end
	return dev
end

function onStart()
	print("Starting")
	device = setup_device(cfg.device);
	device_id = device.id

	mq:sub(cfg.topic_sub, 0)
	mq:bind(cfg.topic_sub, handleTrigger)
end
