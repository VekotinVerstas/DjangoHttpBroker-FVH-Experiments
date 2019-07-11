import struct
from broker.providers.decoder import DecoderProvider


def decode_lsb_float(hex_str):
    FLOAT = 'f'
    binary_data = bytes.fromhex(hex_str)
    fmt = '<' + FLOAT * (len(binary_data) // struct.calcsize(FLOAT))
    vals = struct.unpack(fmt, binary_data)
    return vals[0]


class SolarSwitchBoardDecoder(DecoderProvider):
    description = 'Decode SolarSwitchBoard payload'

    def decode_payload(self, hex_payload, port, **kwargs):
        """
        Payload example:
        e4050000004058460078dc46000003430050114600c03544000010410000254300003042000033430000000003000000

        C Source:
        typedef struct  {
          int16_t batteryVoltageRaw;
          int16_t panelVoltageRaw;
          float mainVoltage_V;
          float panelVoltage_VPV;
          float panelPower_PPV;
          float batteryCurrent_I;
          float yieldTotal_H19;
          float yieldToday_H20;
          float maxPowerToday_H21;
          float yieldYesterday_H22;
          float maxPowerYesterday_H23;
          int errorCode_ERR;
          int stateOfOperation_CS;
        } measurement ;

        :param str hex_payload:
        :param port:
        :param kwargs:
        :return:
        """
        n = 8  # Split hex_payload into 8 character long chunks
        s = [hex_payload[i:i + n] for i in range(0, len(hex_payload), n)]
        data = {
            'mainVoltage_V': decode_lsb_float(s[2]),
            'panelVoltage_VPV': decode_lsb_float(s[3]),
            'panelPower_PPV': decode_lsb_float(s[4]),
            'batteryCurrent_I': decode_lsb_float(s[5]),
            'yieldTotal_H19': decode_lsb_float(s[6]),
            'yieldToday_H20': decode_lsb_float(s[7]),
            'maxPowerToday_H21': decode_lsb_float(s[8]),
        }
        return data
