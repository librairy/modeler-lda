package org.librairy.modeler.lda.utils;

import java.io.*;

/**
 * Created on 31/08/16:
 *
 * @author cbadenes
 */
public class SerializerUtils {

    public static  void serialize(Object object, String path) throws IOException {
        FileOutputStream fout = new FileOutputStream(path);
        ObjectOutputStream out = new ObjectOutputStream(fout);
        out.writeObject(object);
        out.close();
        fout.close();
    }

    public static Object deserialize(String path) throws IOException, ClassNotFoundException {
        FileInputStream fin = new FileInputStream(path);
        ObjectInputStream oin = new ObjectInputStream(fin);
        Object value = oin.readObject();
        oin.close();
        fin.close();
        return value;
    }
}
