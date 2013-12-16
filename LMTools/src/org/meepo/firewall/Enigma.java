package org.meepo.firewall;

import org.apache.log4j.Logger;
import org.apache.ws.commons.util.Base64;
import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.modes.CBCBlockCipher;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.params.ParametersWithIV;

//Enigma is a cipher machine used for encryption and decryption.
public class Enigma {

	public static String encrypt(String content, String cipherString) {
		byte[] key = cipherString.getBytes();
		byte[] iv = "blahFizz2011Buzz".getBytes();
		byte[] plain = content.getBytes();
		String ret = null;

		PaddedBufferedBlockCipher aes = new PaddedBufferedBlockCipher(
				new CBCBlockCipher(new AESEngine()));
		CipherParameters ivAndKey = new ParametersWithIV(new KeyParameter(key),
				iv);
		aes.init(true, ivAndKey);

		try {
			ret = Base64.encode(cipherData(aes, plain));
		} catch (Exception e) {
			logger.error(String.format(
					"Encrypt failed. content:%s. cipher:%s.", content,
					cipherString), e);
		}
		return ret;
	}

	public static String decrypt(String content, String cipherString) {

		String ret = null;

		try {
			byte[] key = cipherString.getBytes();
			byte[] iv = "blahFizz2011Buzz".getBytes();
			byte[] plain = Base64.decode(content);

			PaddedBufferedBlockCipher aes = new PaddedBufferedBlockCipher(
					new CBCBlockCipher(new AESEngine()));
			CipherParameters ivAndKey = new ParametersWithIV(new KeyParameter(
					key), iv);
			aes.init(false, ivAndKey);
			ret = new String(cipherData(aes, plain));

		} catch (Exception e) {
			logger.error(String.format(
					"Decrypt failed. content:%s. cipher:%s.", content,
					cipherString), e);
		}

		return ret;
	}

	private static byte[] cipherData(PaddedBufferedBlockCipher cipher,
			byte[] data) throws Exception {
		int minSize = cipher.getOutputSize(data.length);
		byte[] outBuf = new byte[minSize];
		int length1 = cipher.processBytes(data, 0, data.length, outBuf, 0);
		int length2 = cipher.doFinal(outBuf, length1);
		int actualLength = length1 + length2;
		byte[] result = new byte[actualLength];
		System.arraycopy(outBuf, 0, result, 0, result.length);
		return result;
	}

	/**
	 * convert byte[] to hex string
	 * 
	 * @param buf
	 * @return
	 */
	public static String parseByte2HexStr(byte buf[]) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < buf.length; i++) {
			String hex = Integer.toHexString(buf[i] & 0xFF);
			if (hex.length() == 1) {
				hex = '0' + hex;
			}
			sb.append(hex.toUpperCase());
		}
		return sb.toString();
	}

	/**
	 * convert hex string to byte[]
	 * 
	 * @param hexStr
	 * @return
	 */
	public static byte[] parseHexStr2Byte(String hexStr) {
		if (hexStr.length() < 1)
			return null;
		byte[] result = new byte[hexStr.length() / 2];
		for (int i = 0; i < hexStr.length() / 2; i++) {
			int high = Integer.parseInt(hexStr.substring(i * 2, i * 2 + 1), 16);
			int low = Integer.parseInt(hexStr.substring(i * 2 + 1, i * 2 + 2),
					16);
			result[i] = (byte) (high * 16 + low);
		}
		return result;
	}

	public static Enigma getInstance() {
		return instance;
	}

	private static Enigma instance = new Enigma();
	private static Logger logger = Logger.getLogger(Enigma.class);

	public static void main(String args[]) {
		Enigma enigma = Enigma.getInstance();
		String cipherString = "dnBPrdIMWrJYNGIJ";
		// String cipherString =
		// CipherManager.getInstance().genCipher().getCipherString();
		System.out.println("cipherString  = " + cipherString);
		String src = "/a.txt/nb@gmail.com/rwdms/130000000000";
		System.out.println("src data = " + src);
		String dst = enigma.encrypt(src, cipherString);
		System.out.println("encrypted data = " + dst);

		String fuck = enigma.decrypt(dst, cipherString);
		System.out.println("decrypted data = " + fuck);

	}

}