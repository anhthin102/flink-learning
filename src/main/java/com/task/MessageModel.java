package com.task;

public class MessageModel {
    public MessageModel(){
        DataGenerator d = new DataGenerator();
        this.Id = d.Id();
        this.Quantity=d.Quantity();

    }
    public MessageModel(int i, int q){
        this.Id=i;
        this.Quantity=q;
    }
    public int Id;
    public int Quantity;

    @Override
    public String toString() {

        return "[{\"Id\": \""+Id+"\"," +
                "\"Quantity\": \""+Quantity+"\"" +
                "}]";
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof MessageModel &&
                this.Id == ((MessageModel) other).Id;
    }

}
