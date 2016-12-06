package edu.msViz.mzTree;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import javax.xml.bind.DatatypeConverter;

/**
 * Decodes base64 encoded strings into double arrays
 */
public class Decoder {

    public static void main(String[] args) throws DataFormatException, IOException {
        //sample compressed string.
        String inputComp32 = "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";
        //sample uncompressed string.
        String inputUncomp64 = "eJwt13tcj3f/wPFrq3A73UM5RhfCpuR2nCm6qBzLMWyEax1sQxFiI1yIm+SYYrfSpYNjuFNtRulyyLA0Jm4bcW3KmMOM8rMpfnt8Xp+/no/3+fP9Vg8PiqKYx3vGaoqiqOnjhMZL6ZjxQm3dBKF9JJS+umFC8/EnQv0/8+gvQvuvRbh/JfUPVwuVumux9TahNScD47Lom5fH3s359P33DGYWs7/4GvURvzB3/BnvuVSF5Y5LuY/Wp/WEamIP4lE9qXsMEyrx44j7TiD2/pA4IEJodNtGHJMk1Adkss9nD/taVWJoFfV3q4VagcMy5usJtc7OQr27u9D8sYvQinyX+soJ1DdFEBfF0Od7TKhOPSm0vV4IlXCn5aKvRT2hcRCt482EynwX8idaCvUnEULTYQZzIz8R2o/QyooRqrVo713E/JRN+GA7fX3zmfc4hsop8n7F3Dl8nvuFN/DRffzrJfU4R0O8o6oZ1mspVJzbC/XmHYXqedTquQttF7TiPai7edKf0JP5h72oJ3xAPWew0Fjrx3x9f+JjI6g3DaT/zDjiK6jsG08+Jpj+/h9SnzGZfNEU9sWGyHeiEhVK/ikaI8IxGc1hMzFU+gKVUfP4nDPn0x8Wg4sW46SV1F/Es39wAnM1W+XnTiR/fTt7L6fxrgaZ5Jtk8z31QSPkKPve5DLvkIet0OyK1gdoBMu4RwF7x0lHniLf7yx98agmoXXnO+5rJdRTUPe/Qt9a1NuX0Z+OWr608BbvafIbNnBYIfakNRBqJU2FimcztNHo6izUe7YRqhaaSR2IfTrS/5a70BqO9h0PHO1J3/1e2KE387NR+UrGb9C43J++N97ko33I1xvAO28Mpl7Hn3kNtWvSoYHcvYlGUDDvdwyn/x8LqAetYk/n9cTqNvoGJfE53FO4/2M6+WuZeDObPXoe+/ej9Qz1ncfl93qaeNlF+nN/YN/uq9ydWEbc5AZ+US6/74d8jjuo93hOfwVqbaq41+cV/W/Qbl/DHSfHlWLuXTQHyrh/faHq1pA4QNrJWWg8R2uwK31+7kLlEhrXUHPpxFybbuwP8CMeNoJ6h0BiczTx0LFCe3gwe6aHkB8Uxnwg2hGzuPvxfN4RiWYlGoUL6PtkCc5ZxftcVlNvlsBc6w3kd23jTp8k8k472VeEap8U5oaj1SiDejHqDbPI3z7Inu25zFWi9fYJ+t6gMrmAfU5n6ddRdSthrycancqYf+8W8UDpSLRH3WH+xF3mAyrwojTwEfWp0iNPmXOq4s4MNJyreV8fNGNryPsrq8RcYweh4eFIfAP1YCficNR/ri+0ezakf0ojjGpCfYSzUDmA9vetV/G9oTnWlb4bHYTqTHfyC9H6b1fiuZ7sSUPdtzf9f6B+sy/9D73p/xONNzIe5sP9F368Xx/JfINA5lxQbRUs3zWFuF8Id4Mj0G0O9a4x1OejrsRS778KB6wj3z6Bft8t3J2aSH7MTuZ9UzBsF3X/TOIlB3j/+YPYOZu5P9BslEfcFbV3Crjb9Sz17iXsSb7K/IgyPuck1FNu4thbzE1He2KF/DmglVhJPk265yH7ox/Rt+MxefU5+QVV7IuX9n/FnmGoPEV7Uw0OcogT76tC5aCjUL1an3xAA/L/bEi+uTP57q5CrZ87LvUg/3UvzPAmfwT1SD/cj+Z/R7LXbYLQOoLGMRn7hjAfNZX4BCoB4fSVzqDutIS4/ipU4+i/F4/+6+mbs4X3P9oqtEds4z17Mpj7JJP9SQeZm5NNfX4u8cA83r0O1R0nMO0M9WxpWTFO+Y69WdKwEt7x0yXuXywjflDBnn6V9OU9J86pv1rcK31HqL7XXKgcbUd+e0eh9XUvofYdqjfRuOIt1Ev86C9HbZc/c6VD8YeR5K1g+udOEdorULsQQrz9Y/pjwtjXIZy4HI15s5iPX0B+5hLe82UceyIT2DM3kfmjO6h7pTC3aTfx/Qz6nPewt/Vh6nknqH9xhvwXV9lbLe1/k76WNvcH3yU/u4Lvrc1D5nui0vE57xhVRd+mGupXlDXifrKD0EhyFGoXGwiVLxtSH+RMvgh173ZC64+O9NnueONd6nEe7HvgKVTX96Jf6UP+1HD25oeQb/8xsXs4d/pHsK/LbMxBw4pGbQH9m1B/soj8/SXEucuZ+34V+z+NY+8R1MevoV/Zxp46KbxzXxr1HRnk62eS90Ll/Wyh+SlaUXn0ZZ5gziggPnCGuXOoRZ/lXs53zB8poX69lPrQMlyBZi7ql29xp8XP9IdXsOcamvdQiXxEPq6Kvr3VzEe/YG+XGvrXox3zmr2jHf8t+reg7uaESQ3R0Vmo/MtFaA1Au56r0AhoS59Te+qHOhK7uwu1UNTPeND/nqfQ3I2qX2/2bUQrypu+YT7EMX70hfuz5wgay0eyZ2Yg9TpB5BsEE69C41KwnJtEvC6E+DqaaaHc8QrnHdNnkN+K1uVP+Bx3pH1msycbtZQo7vWdQ37UAmwRy50JaEyO43uMQvPEeuK5CcT7US3cJL+3ROqlaPpvI78Mzb3JOCKFfHgm92Zlc+93VL3z2OOPVjiacTK/C7WDqObL+kVpeIHci9Z6GWei8QOqjQvJZ5zFArQuoOlSjDNQ3VaCB1F7eZX31CljPgSNo/j3f6gw5zr1q2jOucn8hVvsc68gH4XqtErqno/Ix6D92WP586xCP7Q2o7msmrx3Df0jUf93LXm3t9aKfLGD0ApzFGqH0U5zEpoFdajPbChUgxtTr+csNKKlP7YRKo1diePRbN8Wf+5I/T13nIxqJloe7+KvHryjuyd7QlDdg2YO2t26sccP7VG9iU9Lr3kzH+bD/I++5D/zQ1d/7g1CIxLVm0PIDw/k3jzUD4xGJZh+DdWF0g2otw5h/wg0k1BPCeOd/cLpD5UmoZKOdkoE/T/N4h0tZ/N9fIbKr7Pl9xWNt+aTD12Aq9BMlXERGudQK0F75efcH/AF9z6LJY5CPV7G0+OYX4FKBqp3ZfwE7cQE+g6gVoz6I1S9NtBXk0i9xzbe6Y1GkIw/QWU56utk/ktU02V8WvbdlvFD1BomMaehOVbG6TuxXgp+gMZYVBfLeI2sX0SlUvoctVq0G6TS74xqICpH0+XvSwb33TLJj0LtYxnvk+aiZct4chb7Vx1kf9ds9heh8QDt8EPs35RLPCiPu+ul19D+A7XYfPoHH8dhBeybjdp21KvRWl3I3cFnMQytg6ikFpMPOC//rkqIj0rfoJl1Cd8u4z3eqK5EowTNO6gXXiO/7haWodqpHE+j3rmC+kTUtqCdgdbFSurlaL5EK+Oe/Pt7QPzsIfu8HrFnPxqPHpMf9gd7W1cxNxTNOaitQOWszAdUszewhrnp0pG18u8UzdaO6/j+0d6B2i9oznBax94G1DMaUv9NOqcRfb7O7MlHM9oFA1yZ/xPNhm3p093ZF+EptLr3Zt901FegkSrjUjQL+wjV4f3Y29+H+bVo1/Onrxmqsfj3P/zUH6MxOoC8jdrqQOp7pRYqHkHsqUTz7hj6X40n7xFMPBPVU2jeRL12AnHUFN5ZN4S9PVCdK+MLMg6aSr8ZyjuUcObaof056ndQ6xFBfygquTPo7zCbz3kflX6RzE1CM1Lqs4D6CjSUGIyW/rCQO6GL2Ns4ljl/tBeg5riU/ZvQcomjPh3N9ahkotVxNfVUNMMSuJeM6nW00jcQ19tG3QctA81sVCplviCJvropvMstlfgM6h+kMeeeyf101MpQL89iz/x98vcyG9NQ9z3EfMUh+XuYR/4cGjX57BtTQDytEJ1OMtfuLHOfotX2nPx7KeE9yy6Rdy8l9iljfzwa/reo/5+0QTmfPwP1X++yb2AF8TBUp6NdgUaDSuY+v0+9/iPmWqEeIW3+lDmjis/1+0v6b7zivk8N9bXSOw7xYu8SR6FpolEove0kVFPrEd9qILT3NqT/NGoVjYT6E2kzZ+ptpFtQT0f1tPSVC/t+aMGeC67k/doy91Y7odXSHX9GrU1n3u3qybuGSx+g3t6LuG5v9mmobETjDxkbfbBvX/Y38qH+AjXnAcRrpd/4kl/nz/t2BhAHBhEvRjsLjTPSd0bzjsIJvK/1ZPqLp3K3ezh7GkfQH4vqebTKUesWSXwXzeXziYcsoN8jhvuBaNSiPn8R9dClxI7L+Nyxy4l1g/62a9i3YgN7Om6kz3sb8wlJ3B2QTL39TvJ6Kvl0NPx3ke+cQRySSf+eLD7HyD30h2fT98Uh7hah+fFh+m/nkg/NY88yNNOk3+bz/lkF7FmD5h7p3ULqq87yOVKlj4vZf+88cVYJdzpdYk8tap3KmC9EpfYae/1uMV+M1vAK/Bb1u2i73WPP0Ee8vwLV24/Z5/6EuE4VRqBZhtZb1ew5XC1/j19gkxruzETlH7X030TV9zV7VjquF/mDqE6rg6ebkR/iLNS+dBEqJ6WBrkK9Z1uhMQuts2hGdhLau9AsR31jN/bloX0XzSG9yUeiUtuHfte+7N3kgxEDcC8qjgN5705/nBHAXCnarkPon4dmdCBzK4Por0azVsbuo5hfO4F5l4nkfdD6SNo4hD27p9LnN42+cDSbh+N4VP0i2LsftTNo9p3BvhDUyj/j+zwVST4sivmgBdxZiub5GOozFxInxLL/56X0LV9GPnM17/xVGr8GCxPw7Y3sz03ibr1k5t+kss9tF/UWe9g3IZu5nw6Rb38Ye6Fi5vGuZ/nkfb9i35+FfL6Ak/QtO4tNz+FU1C58i91KeM86tEddIl6CuiXjlqXcm4J2ehn7xl8jLkAt9jruvUX/Z+V8nh3SM6ifr2B+fiX9t9By/1V+z4/ZW4zGXVRPPaeeXEX+KJpbq9lXg5brC/buQVOvZd9+NMtQcXhN/KlTgug/Kn2Keus6QsUb7ZEynoSG0UhoXUDjLxl3aMx8mTOuc2FvKVq3UalG4x/NhWYTtD+QTkZtJurbZP6GK/ndbbFVO6E6GM1EN+7ccOfO3k7o15m+j1BfiOa4LryjXlfmirrhx17UHXpT/7YP84v7sq+TD/dj0LgzgPe9ka4YyLyJluFP/hBqiwPYswbNezKOH4I7pCsDqeeg+iaIePco6vnB3M+ewP46EzETtSJUVk8lfwxVt2nUT6GRGMHey6h2mUH+/dnEY9E+jdr+SOLr0m5R5D9cgEdi+J6fSYct5HsIiaXeZSlxEGoX0KiW+bVxvKfHavJN1pCfjPbeBOJhG9g/H43mG8lnbOPdo5PYvxyVQjS7J9P/uXRmKn2JqJ5Gu/8u+pegMTeL/TtR+5903B7esROtdYcwB5Vf0Jh8GH/PYW+HPOqb87l3HJUa1OZ8heelgwuoHyjEH1B7+yT3Y6TDzrIv/6z8/Smm/yfU2pyTfweX5LtRuYjWMxl3KCWei7bL9+yrKpO/d9fIX0bzpYy9r+OzW9zbXI45qP+G1qDbfN8rKpnPQPsCGk9lPOUe/bseYwHqZai9lIY+4d1WNd6vln9vL3BMLVbXyr+v1+zJdtwg7jjVEVr/Qd2/kVCNlr5Cs0lj+tNRK3WmP8YF32ouVLqg/Q0aXq7U17bFVu3Y64X2GRn3ducdGzrR9z9UhnbGIE/uHuhG3/9QH+dF/h7aOX147y8+3P9uAHn/gcxt9ad+OYD6pCHsnxjEnkfSiFHkD6K9fQJzH03kvZFoG6hkTeU90dO4cyyCvqAZ3IlFMxXV4CjitWjViWG+HP/+fwR7oxdiEdoNl1IfhPYvqC1aRqytxj/RarmGvVvQ+AbV6Rvoe3sj+WRUwpJ4V6tkYm9pTbL8/lPoX55K3GEX+kmPZXJ3ShZ7dqM+eQ93l2UTLzzE/V1oTD/M/L585pZ8Rf+DAvlzKyS+jrpxEpNOM1dUzJ7zqO08x/4WF8i/voRmKXveuk79JOrWbWLPJ9x78JS+0mqsfcH3MPg1/Y+dNgqH1hHa1Y2E2uTGOBvtay5CdaGrtC31xe5CJbAzcWk39vX1It/0X8RJPakX9sOlA4VWDurf+xKvCODejlHky1DLmUj9JholM4hTZwnNH6N414H5zC1ayR7HZPJjsvBcNu/65wXiPZfYE1TKHZ9r7J93m3hYJfvyUb1yn73XH7Ln5GPmdzyhv+h36kXPqJ+vZn7qC+aT0LpTQz7nNXMNHDeJ+m/Sb+oIzSuN8V/OmN1cqPyE5iFX+k+0I34mHeOO6Z2E2vedhfozVD70xEnd0KU7/b/3Rdf3hfZfA5nTA7jTZKjQ8pQeCka/CPoGGdzLiSP+aTV7JqzB/mlCw9pP3ese8QqHzeIdDZsJ9W9aC81pHYVGaBeZf1eohXkI7R1exP/uRf+m99nzUT/mdvrSFzqKvii0oqcQW1PpXzCd+c2LuZO8jL7P1+KreFyayNyq7cRxO4VqWCp7muaSd/gac46xL/E0fWUXyW/6jv7E73mfw1XuJ6NuXif/TDrwJvXBd/hczX4nf/85tvg/9j18zZ00hy3inRl1hcq+BkL7SGts6io0u7oR10pzumKKh1BN6kt8vj978ryZy/UltiZSD4kWWkXzqZ9axPy3S7bwc1spNI7HET9ZS9/prbzTfQfxeyns8zDJT91H/+WD5K8eYc+u0/TvLaV+9TL9f9m8y7xP39M2W0V8tZ1Qq3ITmi/7YeYQfDOC+qFAoeEwWqh2Hiu03xkv1F+j1XQSe9/+kLr3ZKwbQt59OpbPY2/jBOInG9nbfDP7/pnIvvg06l5ZvKfmAHPbDtLvcIR8o1zutEGj6dfMqyfIuxex98rf/j/gwvqS";
        //decodes/decompress 32byte array, print
        System.out.println(Arrays.toString(decodeCompressed(inputComp32,false)));
        //decodes 64byte array, print
        System.out.println(Arrays.toString(decodeCompressed(inputUncomp64,true)));

    }

    /**
     * converts base64 string input to array of doubles
     * @param input base64 encoded data
     * @param isDouble flag to signal if data is 32 or 64 bit floating point 
     * @return decoded double array
     */ 
    public static double[] decodeUncompressed(String input, boolean isDouble) {
        return decodeUncompressed(DatatypeConverter.parseBase64Binary(input), isDouble);
    }

    /**
     * converts array of bytes to corresponding array of doubles
     * @param binArray array of bytes
     * @param isDouble flag to signal if data is 32 or 64 bit floating point 
     * @return decoded double array if isCount == false, otherwise a single double, the count of data
     */
    public static double[] decodeUncompressed(byte[] binArray, boolean isDouble) {
        ByteBuffer buf = ByteBuffer.wrap(binArray).order(ByteOrder.LITTLE_ENDIAN);

        int bytesPer = isDouble ? 8 : 4;
        double[] dataArray = new double[binArray.length / bytesPer];

        for (int i = 0; i < dataArray.length; i++) {
             dataArray[i] = isDouble ? buf.getDouble() : buf.getFloat();
        }
        return dataArray;
    }

    /**
     * converts zlib-compressed base-64 encoded data to array of doubles
     * @param encoding base-64 encoded data
     * @param isDouble flag to signal if data is 32 or 64 bit floating point 
     * @return decoded double array
     * @throws java.util.zip.DataFormatException
     * @throws java.io.IOException
     */ 
    public static double[] decodeCompressed(String encoding, boolean isDouble) throws DataFormatException, IOException 
    {
        // Decode the Base64 string into a byte array. 
        byte[] binArray = DatatypeConverter.parseBase64Binary(encoding);

        // decompress zlib	
        Inflater decompressor = new Inflater();
        decompressor.setInput(binArray);

        // Create an expandable byte array to hold the decompressed data
        ByteArrayOutputStream bos = new ByteArrayOutputStream(binArray.length);

        // Decompress the data
        byte[] buf = new byte[1024];
        while (!decompressor.finished()) {
            int count = decompressor.inflate(buf);
            bos.write(buf, 0, count);
        }
        return decodeUncompressed(bos.toByteArray(), isDouble);
    }
    
    /**
     * Counts the number of MS data elements in a compressed base-64 encoded string
     * @param encoding encoded string
     * @param isDouble number of bytes per MS data element
     * @return number of MS data elements in input
     * @throws java.util.zip.DataFormatException 
     */
    public static int countCompressed(String encoding, boolean isDouble) throws DataFormatException
    {
        // overall count
        int count = 0;
        
        // number of bytes per element
        int bytesPer = isDouble ? 8 : 4;
        
        // parse base-64 encoded data into bytes
        byte[] binArray = DatatypeConverter.parseBase64Binary(encoding);

        // decompress zlib	
        Inflater decompressor = new Inflater();
        decompressor.setInput(binArray);
        
        // decompress and count number of bytes
        byte[] buffer = new byte[1024];
        while(!decompressor.finished())
            count += decompressor.inflate(buffer);
        
        // num bytes / num bytes per element
        return count / bytesPer;
        
    }
    
    /**
     *  Counts the number of MS data elements in a compressed base-64 encoded string
     * @param encoding encoded string
     * @param isDouble number of bytes per MS data element
     * @return number of MS data elements in input
     */
    public static int countUncompressed(String encoding, boolean isDouble)
    {
        // num bytes per element
        int bytesPer = isDouble ? 8 : 4;
        
        // num bytes / num bytes per element
        return DatatypeConverter.parseBase64Binary(encoding).length / bytesPer;
    }
}
