package org.apache.solr.schema;

public class ByteDenseVectorParser extends DenseVectorParser {
    private byte[] byteVector;
    private int curPosition;

    public ByteDenseVectorParser(int dimension, Object inputValue, BuilderPhase builderPhase) {
        this.dimension = dimension;
        this.inputValue = inputValue;
        this.builderPhase = builderPhase;
        this.curPosition = 0;
    }

    @Override
    public byte[] getByteVector() {
        if (byteVector == null) {
            byteVector = new byte[dimension];
            parseVector();
        }
        return byteVector;
    }

    @Override
    protected void addNumberElement(Number element) {
        byteVector[curPosition++] = element.byteValue();
    }

    @Override
    protected void addStringElement(String element) {
        byteVector[curPosition++] = Byte.parseByte(element);
    }

    @Override
    protected String errorMessage() {
        return "The expected format is:'[b1,b2..b3]' where each element b is a byte (-128 to 127)";
    }
}