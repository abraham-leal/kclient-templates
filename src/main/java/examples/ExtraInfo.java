/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package examples;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class ExtraInfo extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 7208704841032217913L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"ExtraInfo\",\"namespace\":\"examples\",\"fields\":[{\"name\":\"RandomID\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":\"string\"},{\"name\":\"Favorite_Food\",\"type\":\"string\"},{\"name\":\"Postal_Code\",\"type\":\"int\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<ExtraInfo> ENCODER =
      new BinaryMessageEncoder<ExtraInfo>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<ExtraInfo> DECODER =
      new BinaryMessageDecoder<ExtraInfo>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<ExtraInfo> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<ExtraInfo> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<ExtraInfo> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<ExtraInfo>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this ExtraInfo to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a ExtraInfo from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a ExtraInfo instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static ExtraInfo fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  @Deprecated public int RandomID;
  @Deprecated public java.lang.CharSequence Name;
  @Deprecated public java.lang.CharSequence Favorite_Food;
  @Deprecated public int Postal_Code;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public ExtraInfo() {}

  /**
   * All-args constructor.
   * @param RandomID The new value for RandomID
   * @param Name The new value for Name
   * @param Favorite_Food The new value for Favorite_Food
   * @param Postal_Code The new value for Postal_Code
   */
  public ExtraInfo(java.lang.Integer RandomID, java.lang.CharSequence Name, java.lang.CharSequence Favorite_Food, java.lang.Integer Postal_Code) {
    this.RandomID = RandomID;
    this.Name = Name;
    this.Favorite_Food = Favorite_Food;
    this.Postal_Code = Postal_Code;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return RandomID;
    case 1: return Name;
    case 2: return Favorite_Food;
    case 3: return Postal_Code;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: RandomID = (java.lang.Integer)value$; break;
    case 1: Name = (java.lang.CharSequence)value$; break;
    case 2: Favorite_Food = (java.lang.CharSequence)value$; break;
    case 3: Postal_Code = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'RandomID' field.
   * @return The value of the 'RandomID' field.
   */
  public int getRandomID() {
    return RandomID;
  }


  /**
   * Sets the value of the 'RandomID' field.
   * @param value the value to set.
   */
  public void setRandomID(int value) {
    this.RandomID = value;
  }

  /**
   * Gets the value of the 'Name' field.
   * @return The value of the 'Name' field.
   */
  public java.lang.CharSequence getName() {
    return Name;
  }


  /**
   * Sets the value of the 'Name' field.
   * @param value the value to set.
   */
  public void setName(java.lang.CharSequence value) {
    this.Name = value;
  }

  /**
   * Gets the value of the 'Favorite_Food' field.
   * @return The value of the 'Favorite_Food' field.
   */
  public java.lang.CharSequence getFavoriteFood() {
    return Favorite_Food;
  }


  /**
   * Sets the value of the 'Favorite_Food' field.
   * @param value the value to set.
   */
  public void setFavoriteFood(java.lang.CharSequence value) {
    this.Favorite_Food = value;
  }

  /**
   * Gets the value of the 'Postal_Code' field.
   * @return The value of the 'Postal_Code' field.
   */
  public int getPostalCode() {
    return Postal_Code;
  }


  /**
   * Sets the value of the 'Postal_Code' field.
   * @param value the value to set.
   */
  public void setPostalCode(int value) {
    this.Postal_Code = value;
  }

  /**
   * Creates a new ExtraInfo RecordBuilder.
   * @return A new ExtraInfo RecordBuilder
   */
  public static examples.ExtraInfo.Builder newBuilder() {
    return new examples.ExtraInfo.Builder();
  }

  /**
   * Creates a new ExtraInfo RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new ExtraInfo RecordBuilder
   */
  public static examples.ExtraInfo.Builder newBuilder(examples.ExtraInfo.Builder other) {
    if (other == null) {
      return new examples.ExtraInfo.Builder();
    } else {
      return new examples.ExtraInfo.Builder(other);
    }
  }

  /**
   * Creates a new ExtraInfo RecordBuilder by copying an existing ExtraInfo instance.
   * @param other The existing instance to copy.
   * @return A new ExtraInfo RecordBuilder
   */
  public static examples.ExtraInfo.Builder newBuilder(examples.ExtraInfo other) {
    if (other == null) {
      return new examples.ExtraInfo.Builder();
    } else {
      return new examples.ExtraInfo.Builder(other);
    }
  }

  /**
   * RecordBuilder for ExtraInfo instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<ExtraInfo>
    implements org.apache.avro.data.RecordBuilder<ExtraInfo> {

    private int RandomID;
    private java.lang.CharSequence Name;
    private java.lang.CharSequence Favorite_Food;
    private int Postal_Code;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(examples.ExtraInfo.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.RandomID)) {
        this.RandomID = data().deepCopy(fields()[0].schema(), other.RandomID);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.Name)) {
        this.Name = data().deepCopy(fields()[1].schema(), other.Name);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.Favorite_Food)) {
        this.Favorite_Food = data().deepCopy(fields()[2].schema(), other.Favorite_Food);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.Postal_Code)) {
        this.Postal_Code = data().deepCopy(fields()[3].schema(), other.Postal_Code);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
    }

    /**
     * Creates a Builder by copying an existing ExtraInfo instance
     * @param other The existing instance to copy.
     */
    private Builder(examples.ExtraInfo other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.RandomID)) {
        this.RandomID = data().deepCopy(fields()[0].schema(), other.RandomID);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.Name)) {
        this.Name = data().deepCopy(fields()[1].schema(), other.Name);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.Favorite_Food)) {
        this.Favorite_Food = data().deepCopy(fields()[2].schema(), other.Favorite_Food);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.Postal_Code)) {
        this.Postal_Code = data().deepCopy(fields()[3].schema(), other.Postal_Code);
        fieldSetFlags()[3] = true;
      }
    }

    /**
      * Gets the value of the 'RandomID' field.
      * @return The value.
      */
    public int getRandomID() {
      return RandomID;
    }


    /**
      * Sets the value of the 'RandomID' field.
      * @param value The value of 'RandomID'.
      * @return This builder.
      */
    public examples.ExtraInfo.Builder setRandomID(int value) {
      validate(fields()[0], value);
      this.RandomID = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'RandomID' field has been set.
      * @return True if the 'RandomID' field has been set, false otherwise.
      */
    public boolean hasRandomID() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'RandomID' field.
      * @return This builder.
      */
    public examples.ExtraInfo.Builder clearRandomID() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'Name' field.
      * @return The value.
      */
    public java.lang.CharSequence getName() {
      return Name;
    }


    /**
      * Sets the value of the 'Name' field.
      * @param value The value of 'Name'.
      * @return This builder.
      */
    public examples.ExtraInfo.Builder setName(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.Name = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'Name' field has been set.
      * @return True if the 'Name' field has been set, false otherwise.
      */
    public boolean hasName() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'Name' field.
      * @return This builder.
      */
    public examples.ExtraInfo.Builder clearName() {
      Name = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'Favorite_Food' field.
      * @return The value.
      */
    public java.lang.CharSequence getFavoriteFood() {
      return Favorite_Food;
    }


    /**
      * Sets the value of the 'Favorite_Food' field.
      * @param value The value of 'Favorite_Food'.
      * @return This builder.
      */
    public examples.ExtraInfo.Builder setFavoriteFood(java.lang.CharSequence value) {
      validate(fields()[2], value);
      this.Favorite_Food = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'Favorite_Food' field has been set.
      * @return True if the 'Favorite_Food' field has been set, false otherwise.
      */
    public boolean hasFavoriteFood() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'Favorite_Food' field.
      * @return This builder.
      */
    public examples.ExtraInfo.Builder clearFavoriteFood() {
      Favorite_Food = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'Postal_Code' field.
      * @return The value.
      */
    public int getPostalCode() {
      return Postal_Code;
    }


    /**
      * Sets the value of the 'Postal_Code' field.
      * @param value The value of 'Postal_Code'.
      * @return This builder.
      */
    public examples.ExtraInfo.Builder setPostalCode(int value) {
      validate(fields()[3], value);
      this.Postal_Code = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'Postal_Code' field has been set.
      * @return True if the 'Postal_Code' field has been set, false otherwise.
      */
    public boolean hasPostalCode() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'Postal_Code' field.
      * @return This builder.
      */
    public examples.ExtraInfo.Builder clearPostalCode() {
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ExtraInfo build() {
      try {
        ExtraInfo record = new ExtraInfo();
        record.RandomID = fieldSetFlags()[0] ? this.RandomID : (java.lang.Integer) defaultValue(fields()[0]);
        record.Name = fieldSetFlags()[1] ? this.Name : (java.lang.CharSequence) defaultValue(fields()[1]);
        record.Favorite_Food = fieldSetFlags()[2] ? this.Favorite_Food : (java.lang.CharSequence) defaultValue(fields()[2]);
        record.Postal_Code = fieldSetFlags()[3] ? this.Postal_Code : (java.lang.Integer) defaultValue(fields()[3]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<ExtraInfo>
    WRITER$ = (org.apache.avro.io.DatumWriter<ExtraInfo>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<ExtraInfo>
    READER$ = (org.apache.avro.io.DatumReader<ExtraInfo>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeInt(this.RandomID);

    out.writeString(this.Name);

    out.writeString(this.Favorite_Food);

    out.writeInt(this.Postal_Code);

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.RandomID = in.readInt();

      this.Name = in.readString(this.Name instanceof Utf8 ? (Utf8)this.Name : null);

      this.Favorite_Food = in.readString(this.Favorite_Food instanceof Utf8 ? (Utf8)this.Favorite_Food : null);

      this.Postal_Code = in.readInt();

    } else {
      for (int i = 0; i < 4; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.RandomID = in.readInt();
          break;

        case 1:
          this.Name = in.readString(this.Name instanceof Utf8 ? (Utf8)this.Name : null);
          break;

        case 2:
          this.Favorite_Food = in.readString(this.Favorite_Food instanceof Utf8 ? (Utf8)this.Favorite_Food : null);
          break;

        case 3:
          this.Postal_Code = in.readInt();
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}









